import json
import re
from openai import AzureOpenAI
from config import AI_ENDPOINT, AI_API_KEY, AI_MODEL


client = None
if AI_API_KEY and AI_ENDPOINT:
    client = AzureOpenAI(
        api_key=AI_API_KEY,
        azure_endpoint=AI_ENDPOINT,
        api_version="2024-05-01-preview"
    )

# File extensions that should never be mistaken for a SAP HANA
# "schema.table" reference (used by the dotted-notation regex below).
_NON_TABLE_EXTENSIONS = {"csv", "xlsx", "xls", "json", "txt", "parquet", "tsv"}


# ==========================
# FALLBACK PARSER (PRIMARY)
# ==========================
def fallback_parser(prompt: str):
    prompt_lower = prompt.lower()

    sources = []
    sap_seen = set()

    def add_sap_source(table, schema):
        key = (schema, table)
        if key not in sap_seen:
            sap_seen.add(key)
            sources.append({
                "type": "sap_hana",
                "schema": schema,
                "table": table
            })

    # ----------- S3 PARSING -----------
    # matches: bucket-name/file.csv OR s3://bucket/file.csv
    s3_matches = re.findall(r"(?:s3://)?([\w\-]+)/([\w\-.]+\.csv)", prompt_lower)

    for bucket, file_name in s3_matches:
        sources.append({
            "type": "s3",
            "bucket": bucket,
            "file_name": file_name
        })

    # ----------- SAP HANA PARSING -----------
    # Supports several natural phrasings — adjust/extend these once you
    # confirm the exact wording your users actually use:
    #   "take MARA table from SAPABAP1 schema"
    #   "table MARA from schema SAPABAP1"
    #   "schema SAPABAP1 table MARA"
    #   "SAPABAP1.MARA" (dotted schema.table notation)

    # Pattern: "<table1>, <table2> ... and <tableN> table(s) from <schema> schema"
    # Lets the schema be mentioned once for a whole list of tables, e.g.
    #   "take Employees, Equipments, Plants, Vendors and Work_Orders
    #    tables from VERITON schema"
    _STOPWORDS = {"take", "please", "get", "fetch", "also", "the", "and"}

    for table_list, schema in re.findall(
        r"\b([\w][\w,\s]*?)\s+tables?\s+from\s+([\w]+)\s+schema\b", prompt_lower
    ):
        # Split on commas and/or any whitespace (table names are single
        # tokens, e.g. "work_orders", so this is safe) — this also
        # separates leading filler words like "take" from the first
        # table name instead of leaving them glued together.
        tokens = re.split(r'[,\s]+', table_list.strip())
        for tok in tokens:
            tok = tok.strip()
            if not tok or tok in _STOPWORDS:
                continue
            add_sap_source(tok, schema)

    # Pattern: "<table> table from <schema> schema"
    for table, schema in re.findall(
        r"\b([\w]+)\s+table\s+from\s+([\w]+)\s+schema\b", prompt_lower
    ):
        add_sap_source(table, schema)

    # Pattern: "table <table> from schema <schema>"
    for table, schema in re.findall(
        r"\btable\s+([\w]+)\s+from\s+schema\s+([\w]+)\b", prompt_lower
    ):
        add_sap_source(table, schema)

    # Pattern: "schema <schema> table <table>"
    for schema, table in re.findall(
        r"\bschema\s+([\w]+)\s+table\s+([\w]+)\b", prompt_lower
    ):
        add_sap_source(table, schema)

    # Pattern: dotted "schema.table" notation, e.g. "sapabap1.mara"
    # Skip anything that looks like a filename (common extensions), so
    # this doesn't collide with S3/Azure file references.
    for schema, table in re.findall(
        r"\b([a-z_][\w]*)\.([a-z_][\w]*)\b", prompt_lower
    ):
        if table in _NON_TABLE_EXTENSIONS:
            continue
        add_sap_source(table, schema)

    # ----------- AZURE PARSING -----------
    azure_files = re.findall(r"\b([\w\-.]+\.csv)\b", prompt_lower)

    for file_name in azure_files:
        # avoid duplicates already captured in S3
        if not any(s.get("file_name") == file_name for s in sources):
            sources.append({
                "type": "azure",
                "file_name": file_name
            })

    return {"sources": sources}


# ==========================
# OPTIONAL AI PARSER
# ==========================
def ai_parser(prompt: str):
    response = client.chat.completions.create(
        model=AI_MODEL,
        messages=[
            {
                "role": "system",
                "content": """
Extract data sources mentioned in the prompt.

Return STRICT JSON:
{
  "sources": [
    {"type": "s3", "bucket": "my-bucket", "file_name": "customers.csv"},
    {"type": "azure", "file_name": "sales.csv"},
    {"type": "sap_hana", "schema": "SAPABAP1", "table": "MARA"}
  ]
}

Rules:
- "s3" sources have a bucket and a file_name ending in .csv.
- "azure" sources have only a file_name ending in .csv (no bucket).
- "sap_hana" sources have a schema and a table (SAP HANA database
  objects, e.g. "SAPABAP1.MARA" or "take the MARA table from the
  SAPABAP1 schema" or "table MARA from schema SAPABAP1"). Never give
  sap_hana sources a file_name.
- Only include sources actually mentioned in the prompt.
"""
            },
            {"role": "user", "content": prompt}
        ],
        temperature=0
    )

    return json.loads(response.choices[0].message.content)


# ==========================
# MAIN PARSER
# ==========================
def parse_prompt(prompt: str):

    # 1️⃣ Rule-based first
    result = fallback_parser(prompt)

    if result["sources"]:
        print("✅ Using fallback parser")
        return result

    # 2️⃣ AI fallback
    if client:
        try:
            print("🤖 Using OpenAI parser")
            return ai_parser(prompt)
        except Exception as e:
            print("⚠️ OpenAI failed:", str(e))

    raise Exception("No sources detected in prompt")
