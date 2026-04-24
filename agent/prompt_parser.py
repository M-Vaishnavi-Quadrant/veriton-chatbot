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


# ==========================
# FALLBACK PARSER (PRIMARY)
# ==========================
def fallback_parser(prompt: str):
    prompt_lower = prompt.lower()

    sources = []

    # ----------- S3 PARSING -----------
    # matches: bucket-name/file.csv OR s3://bucket/file.csv
    s3_matches = re.findall(r"(?:s3://)?([\w\-]+)/([\w\-.]+\.csv)", prompt_lower)

    for bucket, file_name in s3_matches:
        sources.append({
            "type": "s3",
            "bucket": bucket,
            "file_name": file_name
        })

    # ----------- AZURE PARSING -----------
    azure_files = re.findall(r"\b([\w\-.]+\.csv)\b", prompt_lower)

    for file_name in azure_files:
        # avoid duplicates already captured in S3
        if not any(s["file_name"] == file_name for s in sources):
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
Extract sources.

Return STRICT JSON:
{
  "sources": [
    {"type": "s3", "bucket": "my-bucket", "file_name": "customers.csv"},
    {"type": "azure", "file_name": "sales.csv"}
  ]
}
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
