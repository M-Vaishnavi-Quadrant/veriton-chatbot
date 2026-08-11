"""
Shared utility functions for Azure Functions
"""

import logging
import json
import re
from datetime import datetime, timezone
import pandas as pd
import io
import os
import numpy as np
import time
from collections import Counter

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    from azure.storage.blob import BlobServiceClient
    from azure.identity import DefaultAzureCredential
    from openai import AzureOpenAI
except ImportError as e:
    logger.error(f"Failed to import Azure dependencies: {e}")
    raise

# ====================================================================
# ENVIRONMENT VARIABLES
# ====================================================================
STORAGE_ACCOUNT_NAME = os.environ.get("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY  = os.environ.get("STORAGE_ACCOUNT_KEY")
LAKEHOUSE_CONTAINER  = os.environ.get("LAKEHOUSE_CONTAINER", "lakehouse-data")
DDL_CONTAINER        = os.environ.get("DDL_CONTAINER", "ddl-scripts")

USER_CONTAINER_NAME = os.getenv("USER_CONTAINER_NAME", "userdata")
RELATIONSHIPS_CONTAINER_NAME = os.getenv("RELATIONSHIPS_CONTAINER_NAME", "relationships")
NORMALIZED_CONTAINER_NAME    = os.getenv("NORMALIZED_CONTAINER_NAME", "normalized")
METADATA_CONTAINER_NAME      = os.getenv("METADATA_CONTAINER_NAME", "metadata")

AI_ENDPOINT = os.environ.get("AZURE_OPENAI_ENDPOINT")
AI_API_KEY  = os.environ.get("AZURE_OPENAI_API_KEY")
AI_MODEL    = os.environ.get("AZURE_OPENAI_DEPLOYMENT", "gpt-4o-mini")

WORKSPACE_ID = os.environ.get("FABRIC_WORKSPACE_ID")
WAREHOUSE_ID = os.environ.get("WAREHOUSE_ID")

SUPPORTED_FORMATS = [".csv", ".parquet", ".json"]

if not STORAGE_ACCOUNT_NAME:
    logger.warning("STORAGE_ACCOUNT_NAME not set - this may cause issues")
if not STORAGE_ACCOUNT_KEY:
    logger.warning("STORAGE_ACCOUNT_KEY not set - this may cause issues")

BLOB_CONN_STR = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)

# ====================================================================
# CONSTANTS
# ====================================================================
# A column with null_percentage above this threshold is NEVER a key
NULL_THRESHOLD_FOR_KEY = 5.0

# Relationships below this confidence score are rejected
CONFIDENCE_THRESHOLD = 0.7

# Decomposition consistency: an "ID-pattern" column with a distinct/row ratio
# below this threshold repeats across rows and is therefore a dimension key,
# not this table's own identity — even if the AI never flagged the table as
# denormalized.
ID_CARDINALITY_THRESHOLD = 0.5

# Don't run cardinality-based decomposition checks on very small tables —
# not enough rows to trust a distinct/row ratio.
MIN_ROWS_FOR_DECOMPOSITION_CHECK = 20

# A table needs at least this many low-cardinality ID-pattern columns before
# we treat it as denormalized on our own initiative.
MIN_LOW_CARD_ID_COLS_FOR_DECOMPOSITION = 2

def _normalize_name(name: str) -> str:
    """Normalize names for comparison only. Never use output for display."""
    if not name:
        return ""
    return name.lower().strip().replace(" ", "_").replace("-", "_")

def _derived_from_list(entity: dict) -> list:
    """Split a possibly comma-joined derived_from into normalized parts.
    Handles both a single source ('Orders') and a merged fact_table
    ('EmployeeAttendance, HoroscopePredictions, ...')."""
    raw = entity.get("derived_from", "") or ""
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return [_normalize_name(p) for p in parts]


def _resolve_source_schemas(entity: dict, schema_map: dict) -> list:
    """Return every source schema this entity was derived from —
    one for a normal entity, several for a Rule-9b merged fact_table."""
    return [
        schema_map[d] for d in _derived_from_list(entity)
        if d in schema_map
    ]


def _is_id_pattern_column(col_name: str, pk_norm_set: set = None) -> bool:
    """
    Heuristic: does this column look like a dimension identifier
    (e.g. MachineID, LineID, OperatorID, customer_id)?
    Excludes columns that are already this table's own declared PK.
    """
    if not col_name:
        return False
    norm = _normalize_name(col_name)
    if pk_norm_set and norm in pk_norm_set:
        return False
    return bool(re.search(r"(^|_)id$", norm)) or norm.endswith("id")


# ====================================================================
# BLOB OPERATIONS
# ====================================================================
def list_blobs(container_name, prefix=""):
    """List blobs in a container with optional prefix."""
    try:
        blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
        container_client = blob_service_client.get_container_client(container_name)
        return list(container_client.list_blobs(name_starts_with=prefix))
    except Exception as e:
        logging.error(f"Error listing blobs: {e}")
        return []


def load_blob_json(container, blob_name):
    client = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    blob_client = client.get_container_client(container).get_blob_client(blob_name)
    data = blob_client.download_blob().readall()
    return json.loads(data)


# ====================================================================
# CUSTOM JSON ENCODER FOR NUMPY TYPES
# ====================================================================
class NumpyEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles NumPy types"""
    def default(self, obj):
        if isinstance(obj, np.bool_):
            return bool(obj)
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NumpyEncoder, self).default(obj)


def read_blob_if_exists(blob_name):
    """Read a blob if it exists, otherwise return None."""
    
    
    service   = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    container = service.get_container_client(METADATA_CONTAINER_NAME)
    client    = container.get_blob_client(blob_name)
    try:
        data = client.download_blob().readall().decode()
        return json.loads(data)
    except Exception:
        return None


def save_to_blob(content, blob_name, container_name):
    """Save content to Azure Blob Storage."""
    
    blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=blob_name
    )
    blob_client.upload_blob(content, overwrite=True)


# ====================================================================
# ER MODEL VALIDATION
# ====================================================================
def validate_er_model(er_model: dict) -> list:
    """
    Validates the ER model output before it is saved or consumed.

    Returns a list of error strings. Empty list = valid.
    Critical errors (prefixed CRITICAL) should block saving.
    Warnings (prefixed WARNING) are logged but do not block.
    """
    errors = []

    tables = er_model.get("tables", [])
    if not tables:
        errors.append("CRITICAL: No entities found in ER model.")
        return errors  # No point continuing

    entity_names = set()
    for t in tables:
        name = t.get("table_name") or t.get("table") or "<unnamed>"
        entity_names.add(name)
        
        if not t.get("primary_keys"):
            if not t.get("surrogate_keys"):
                standalone_names = {
                    s if isinstance(s, str) else s.get("table", "")
                    for s in er_model.get("standalone_entities", [])
                }
                if name == "fact_table" or t.get("table_type") == "FACT":
                    pass
                elif name in standalone_names:
                    pass  # standalone entity — kept as-is, no PK requirement forced
                elif t.get("is_normalized") is False and any(
                    e.get("derived_from") == name
                    for e in er_model.get("tables", [])
                ):
                    pass
                else:
                    errors.append(f"WARNING: Entity '{name}' has no primary key or surrogate key.")
        # Surrogate keys must NOT appear in source_columns
        surrogate_set = set(t.get("surrogate_keys", []))
        source_set    = set(t.get("source_columns", []))
        overlap = surrogate_set & source_set
        if overlap:
            errors.append(
                f"WARNING: Entity '{name}' has columns listed as both surrogate and source: {overlap}"
            )

    # Relationships must reference known entities
    for rel in er_model.get("relationships", []):
        from_e = rel.get("from_table", "")
        to_e   = rel.get("to_table", "")
        conf   = rel.get("confidence", 1.0)

        if from_e not in entity_names:
            errors.append(f"CRITICAL: Relationship references unknown entity '{from_e}'")
        if to_e not in entity_names:
            errors.append(f"CRITICAL: Relationship references unknown entity '{to_e}'")
        if not rel.get("relationship_type"):
            errors.append(f"WARNING: Relationship {from_e}→{to_e} is missing type.")
        if conf is not None and conf < CONFIDENCE_THRESHOLD:
            errors.append(
                f"CRITICAL: Relationship {from_e}→{to_e} has confidence {conf} below threshold "
                f"{CONFIDENCE_THRESHOLD} — should have been filtered before saving."
            )

    return errors

def _estimate_max_tokens(schemas_for_prompt, base=6000, per_column=150, ceiling=30000):
    col_count = sum(len(s.get("columns", [])) for s in schemas_for_prompt)
    return min(ceiling, base + per_column * col_count)

# ====================================================================
# RELATIONSHIP DETECTION  (AI-powered with fallback)
# ====================================================================
def detect_relationships(schemas, source_dfs=None):
    """
    Detect relationships and produce a normalized ER model using AI.
    Falls back to rule-based detection if AI fails or returns bad JSON.
    """
    logging.info(f"🤖 Using AI to build ER model for {len(schemas)} tables")

    client = AzureOpenAI(
        api_key=AI_API_KEY,
        api_version="2024-05-01-preview",
        azure_endpoint=AI_ENDPOINT
    )

    # Build a richer input for the AI — include null_percentage so it can
    # honour the high-null-column rule itself as well
    formatted_schemas = []
    for schema in schemas:
        table_name = schema.get("table_name", "")
        columns    = schema.get("columns", [])
        formatted_schemas.append({
            "table_name": table_name,
            "row_count":  schema.get("row_count", 0),
            "columns": [
                {
                    "name":            col.get("column_name", ""),
                    "data_type":       col.get("data_type", ""),
                    "null_percentage": round(col.get("null_percentage", 0.0), 2),
                    "distinct_count":  col.get("distinct_count", 0),
                    "cardinality_pct": round(col.get("cardinality_percentage", 0.0), 2),
                    "is_potential_key": col.get("is_potential_key", False)
                }
                for col in columns
            ]
        })

    prompt = (
        "You are a Senior Data Architect specializing in data modeling and normalization.\n\n"
        "Analyze the provided database schemas and return a normalized ER model.\n\n"

        "🔒 Hard Rules:\n"
        "1. Use ONLY columns present in the input schemas.\n"
        "2. Do NOT invent attributes not derivable from the source.\n"
        "3. NEVER use a column with null_percentage > 5 as a primary key.\n"
        "4. If a table is denormalized (multiple conceptual entities in one table), decompose it.\n"
        "5. For decomposed entities with no natural PK, assign a surrogate key "
        "   Every entity MUST have all its columns listed in attributes[], "
        "including composite key columns. Never leave attributes[] empty.\n"
        "(e.g. StartupID, InvestorID). Mark these with is_surrogate: true.\n"
        "6. Surrogate key names must NOT match any existing source column name.\n"
        "6b. FK relationships MUST use the FK column on the child side as from_column, "
        "never the PK of the child. "
        "Example: Order.customer_code → Customer.customer_code is correct. "
        "Order.order_id → Customer.customer_code is WRONG.\n"
        "7. For every relationship, assign a confidence score 0.0–1.0. "
        "OMIT any relationship where confidence < 0.7.\n"
        "8. Output ONLY valid JSON. No markdown. No explanation outside the JSON.\n\n"
        "8b. Every string value, including 'cardinality_diagram', must be a single "
        "continuous JSON string. NEVER use '+' to concatenate strings, and NEVER use "
        "JavaScript/Python string-concatenation or template-literal syntax. Use '\\n' "
        "inside one quoted string for line breaks instead of splitting it across "
        "multiple '+'-joined strings. Output must be strict RFC 8259 JSON, parseable "
        "by json.loads() with zero preprocessing or repair.\n\n"
        "9. Name the fact entity 'fact_table'.\n"
        "9b. If multiple input tables each have their own numeric measures "
        "and outgoing foreign keys, and do NOT reference each other, they "
        "represent different event types at a comparable grain — do NOT "
        "pick just one as fact_table. Instead define a SINGLE fact_table "
        "that is the union of all of them, with:\n"
        "    - an 'event_type' attribute (one distinct value per source "
        "      table, e.g. 'PRODUCTION', 'QUALITY', 'ATTENDANCE', 'SALES')\n"
        "    - a 'source_record_id' attribute holding that row's original "
        "      primary key from its source table, for traceability\n"
        "    - shared attribute names ONLY where the meaning is genuinely "
        "      the same across sources (e.g. a date column used by all); "
        "      source-specific measures/attributes may be null on rows "
        "      from a different event_type\n"
        "    Note in observations which source table each event_type's "
        "    row-shape came from.\n"
        
        "10. If an entity is nominated as the fact candidate, do NOT also create a separate "
        "    DIM entity derived from the same source with overlapping FK columns. "
        "    The fact entity IS that grain — it must not have a dimension twin.\n"
        "10b. If one fact-like source table references another fact-like "
        "source table's own primary key (not a shared dimension), and both "
        "were merged under Rule 9b, model that as a self-referencing "
        "'parent_record_id' attribute on fact_table — do NOT invent a "
        "separate dimension for it.\n"
        "11. Keep date/datetime columns as regular attributes on the entity "
        "they came from (e.g. fact_table). Do NOT extract a separate date_dimension "
        "unless the source data itself is already a standalone calendar/date table."
        "11b. If the fact entity contains string descriptive columns that are not FKs "
        "     (e.g. product name, status, category), keep them on the fact table as "
        "     degenerate dimensions. Do NOT drop them silently.\n"
        "11c. When extracting numeric measures for the fact entity, exclude columns "
        "     that are clearly identifiers or descriptive attributes such as phone "
        "     numbers, zip codes, years used as labels, or any column whose name "
        "     contains 'phone', 'zip', 'code', 'year' unless it is explicitly a "
        "     measure like revenue or count.\n"
        "12. If NO relationships exist between ALL tables, mark them as standalone_entities. "
        "EXCEPTION: a table that was merged into fact_table under Rule 9b is NOT standalone — "
        "do not list it in standalone_entities even if it has no direct FK relationships to "
        "other remaining entities.\n"
        "12b. If no relationships exist, do NOT mention choosing a fact entity in observations. "
        "     Only state that no relationships were found and list the standalone entities.\n"
        "13. Every entity referenced in 'relationships' MUST also be defined in "
        "    'normalized_entities'. Never reference an entity in a relationship "
        "    that you have not fully defined. If you cannot define the entity, "
        "    omit the relationship entirely.\n"

        "Steps:\n"
        "Step 1 — Analyse each table: identify if it is denormalized and what conceptual "
        "entities are embedded in it.\n"
        "Step 2 — Decompose denormalized tables into normalized entities with proper PKs/FKs.\n"
        "Step 2b — Choose ONE modeling approach and stick to it:\n"
        "  • If exactly one input table is the natural highest grain "
        "    (has FKs + measures, and other tables reference it), that one "
        "    becomes fact_table.\n"
        "  • If MULTIPLE input tables are each fact-like on their own and "
        "    don't reference each other, apply Rule 9b: union them into "
        "    one fact_table with an event_type discriminator instead of "
        "    picking a single winner.\n"
        "  • Name it fact_table.\n"
        "  • Do NOT also create a DIM entity for that same grain from the same source.\n"
        "  • Do NOT create both a bridge/junction table AND a fact entity for the same grain.\n"
"  • Do NOT create both a bridge/junction table AND a fact entity for the same grain.\n"
        "Step 3 — Determine relationships (1:1, 1:M, M:N) between entities.\n"
        "Step 4 — Assign confidence scores to each relationship.\n"
        "Step 5 — Build a single-line cardinality diagram.\n\n"

        "Output format (JSON ONLY, no other text):\n"
        "{\n"
        '  "raw_entity_analysis": {\n'
        '    "TableName": {\n'
        '      "source_file": "filename.csv",\n'
        '      "row_count": 0,\n'
        '      "is_denormalized": true,\n'
        '      "embedded_concepts": ["Concept1", "Concept2"],\n'
        '      "columns": [\n'
        '        {\n'
        '          "name": "col",\n'
        '          "data_type": "string",\n'
        '          "null_percentage": 0.0,\n'
        '          "observations": "e.g. mostly null — excluded from key inference"\n'
        '        }\n'
        '      ]\n'
        '    }\n'
        '  },\n'
        '  "normalized_entities": {\n'
        '    "EntityName": {\n'
        '      "derived_from": "OriginalTableName",\n'
        '      "primary_key": ["ColumnName"],\n'
        '      "composite_key": [],\n'
        '      "attributes": [\n'
        '        {\n'
        '          "name": "ColName",\n'
        '          "data_type": "string",\n'
        '          "is_foreign_key": false,\n'
        '          "is_surrogate": false,\n'
        '          "references": null\n'
        '        }\n'
        '      ]\n'
        '    }\n'
        '  },\n'
        '  "relationships": [\n'
        '    {\n'
        '      "from_entity": "ChildEntity",\n'
        '      "to_entity": "ParentEntity",\n'
        '      "type": "1:M",\n'
        '      "from_column": "FKCol",\n'
        '      "to_column": "PKCol",\n'
        '      "description": "One ParentEntity has many ChildEntities",\n'
        '      "confidence": 0.95\n'
        '    }\n'
        '  ],\n'
        '  "cardinality_diagram": "Industry 1 ────< Startup 1 ────< Investment >──── 1 Investor",\n'
        '  "standalone_entities": [],\n'
        '  "observations": [\n'
        '    "Free-text notes on data quality, missing PKs, high-null columns, or modeling decisions"\n'
        '  ]\n'
        "}\n\n"

        "Decision Policy:\n"
        "- When uncertain → do NOT create a relationship.\n"
        "- Confidence < 0.7 → omit the relationship entirely.\n"
        "- High-null columns (null_percentage > 5) → never use as PK.\n"
        "- Note all decisions and data quality issues in the observations array.\n\n"

        "SCHEMAS:\n"
        f"{json.dumps(formatted_schemas, indent=2, cls=NumpyEncoder)}"
    )
    content = None
    finish_reason = None
    try:
        logging.info("📤 Sending schemas to Azure OpenAI for ER modeling...")

        max_retries = 5
        for attempt in range(max_retries):
            try:
                response = client.chat.completions.create(
                    model=AI_MODEL,
                    messages=[
                        {"role": "system", "content": "Return ONLY valid JSON. No markdown. No explanations. No trailing commas."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0,
                    max_tokens=_estimate_max_tokens(formatted_schemas)
                )
                content       = response.choices[0].message.content.strip()
                finish_reason = response.choices[0].finish_reason
                logging.info(f"📥 AI response received | finish_reason={finish_reason} | length={len(content)} chars")
                ai_result = _extract_json_from_text(content)

                logging.info("✅ AI ER model received — validating structure...")
                result = _transform_ai_result_to_standard_format(ai_result, schemas)
                return verify_and_clean_model(result, schemas, source_dfs or {})

            except Exception as e:
                if "429" in str(e) or "rate_limit" in str(e).lower():
                    if attempt < max_retries - 1:
                        wait_time = (2 ** attempt) * 5
                        logging.warning(
                            f"⚠️ Rate limited. Waiting {wait_time}s "
                            f"(attempt {attempt + 1}/{max_retries})"
                        )
                        time.sleep(wait_time)
                    else:
                        logging.error("❌ Max retries reached. Using fallback.")
                        raise
                else:
                    raise

    except Exception as e:
        logging.exception("❌ AI ER modeling failed. Using rule-based fallback.")
        if content:
            logging.error(f"🔎 RAW AI RESPONSE (finish_reason={finish_reason}, length={len(content)} chars):\n{content}")
        return _fallback_relationship_detection(schemas)


def _transform_ai_result_to_standard_format(ai_result, schemas):
    """
    Validates and transforms the AI ER output into the pipeline's standard format.

    Key guarantees:
    - Required top-level keys are present (raises ValueError if not)
    - Relationships below CONFIDENCE_THRESHOLD are rejected
    - Surrogate keys are cross-checked against actual source columns
    - is_surrogate is set deterministically, not trusted blindly from AI
    - model_source is set to "ai"
    """

    # ------------------------------------------------------------------
    # 1. Validate required top-level keys
    # ------------------------------------------------------------------
    required_keys = {"normalized_entities", "relationships"}
    missing = required_keys - set(ai_result.keys())
    if missing:
        raise ValueError(
            f"AI response is missing required keys: {missing}. "
            f"Triggering fallback."
        )

    normalized_entities = ai_result.get("normalized_entities", {})
    raw_relationships   = ai_result.get("relationships", [])
    observations        = ai_result.get("observations", [])

    if not isinstance(normalized_entities, dict):
        raise ValueError("'normalized_entities' must be a dict. Triggering fallback.")
    if not isinstance(raw_relationships, list):
        raise ValueError("'relationships' must be a list. Triggering fallback.")

    # ------------------------------------------------------------------
    # 2. Build ground-truth set of real source column names
    #    (used to deterministically identify surrogate keys)
    # ------------------------------------------------------------------
    source_column_names = {
        col.get("column_name", "").strip().lower()
        for schema in schemas
        for col in schema.get("columns", [])
    }

    # ------------------------------------------------------------------
    # 3. Build tables_info from normalized entities
    # ------------------------------------------------------------------
    tables_info = []

    for entity_name, entity_data in normalized_entities.items():
        if not isinstance(entity_data, dict):
            logging.warning(f"⚠️ Skipping malformed entity: {entity_name}")
            observations.append(f"WARNING: Skipped malformed entity definition for '{entity_name}'")
            continue

        primary_key = entity_data.get("primary_key", [])
        attributes  = entity_data.get("attributes", [])

        if not isinstance(attributes, list):
            attributes = []

        surrogate_keys = []
        source_columns = []
        foreign_keys   = []

        for attr in attributes:
            if not isinstance(attr, dict):
                continue

            attr_name      = attr.get("name", "")
            attr_name_low  = attr_name.strip().lower()
            
            is_surrogate_deterministic = attr_name_low not in source_column_names

            attr["is_surrogate"] = is_surrogate_deterministic
            


            # Add display metadata for frontend
            if is_surrogate_deterministic:
                attr["source"]        = "ai_generated"
                attr["display_label"] = f"{attr_name} ✨"
                attr["tooltip"]       = (
                    "This key was generated by AI during normalization. "
                    "It does not exist in your source file."
                )
                surrogate_keys.append(attr_name)
            else:
                # Find which source file this column came from
                source_file = _find_source_file(attr_name_low, schemas, preferred_table=entity_data.get("derived_from", ""))
                attr["source"]        = source_file
                attr["display_label"] = attr_name
                attr["tooltip"]       = f"Source column from {source_file}"
                source_columns.append(attr_name)

            # Build FK list
            if attr.get("is_foreign_key", False):
                raw_ref = attr.get("references", "") or ""
                if not raw_ref:  # skip FK with no reference
                    continue
                # Handle both string format ("Table.column") and dict format ({"table": "X", "column": "Y"})
                if isinstance(raw_ref, dict):
                    ref_table = raw_ref.get("table") or raw_ref.get("entity") or raw_ref.get("name") or ""
                    ref_col   = raw_ref.get("column") or raw_ref.get("field") or None
                elif isinstance(raw_ref, str) and "." in raw_ref:
                    ref_table, ref_col = raw_ref.split(".", 1)
                elif isinstance(raw_ref, str):
                    ref_table, ref_col = raw_ref, None
                else:
                    logging.warning(f"⚠️ Unrecognized references format for {attr_name}: {raw_ref}")
                    continue

                if not ref_table:
                    continue

                foreign_keys.append({
                    "column":            attr_name,
                    "references_table":  _normalize_name(ref_table),
                    "references_column": ref_col.strip() if isinstance(ref_col, str) else ref_col
                })
        # ---------------------------------------------------------
       
        # Only first surrogate should be PK — rest are regular attributes
        if len(surrogate_keys) > 1:
            observations.append(
                f"WARNING: {entity_name} had {len(surrogate_keys)} surrogates — "
                f"keeping only '{surrogate_keys[0]}' as PK surrogate"
            )
            for a in attributes:
                if a.get("is_surrogate") and a.get("name") != surrogate_keys[0]:
                    a["is_surrogate"] = False
                    a["display_label"] = a["name"]
                    a["tooltip"] = f"Source column from {entity_data.get('derived_from', 'unknown')}"
            surrogate_keys = [surrogate_keys[0]]

            

        tables_info.append({
            "table":          entity_name,
            "derived_from":   entity_data.get("derived_from", ""),
            "primary_keys":   primary_key,
            "surrogate_keys": surrogate_keys,
            "source_columns": source_columns,
            "foreign_keys":   foreign_keys,
            "attributes":     attributes
        })

        # --- Degenerate dimension recovery (fact_table only) ---
        if _normalize_name(entity_name) == "fact_table":
            derived = entity_data.get("derived_from", "")
            source_schema = next(
                (s for s in schemas if s.get("table_name", "").lower() == derived.lower()),
                None
            )
            if source_schema:
                pk_names       = {_normalize_name(p) for p in entity_data.get("primary_key", [])}
                fk_names       = {_normalize_name(f["column"]) for f in foreign_keys}
                existing_names = {a.get("name", "").lower() for a in attributes}
                already_tracked = existing_names | pk_names | fk_names

            # Skip date columns if DateKey already exists as a FK
            

                for col in source_schema.get("columns", []):
                    cname = col.get("column_name", "")
                    if _normalize_name(cname) not in already_tracked:
                        attributes.append({
                            "name":          cname,
                            "data_type":     col.get("data_type", "string"),
                            "is_foreign_key": False,
                            "is_surrogate":  False,
                            "references":    None,
                            "source":        derived,
                            "display_label": cname,
                            "tooltip":       f"Source column from {derived}"
                        })
                        source_columns.append(cname)
                        observations.append(
                            f"INFO: Re-injected degenerate dimension '{cname}' "
                            f"onto fact_table from source '{derived}'"
                        )

    # ------------------------------------------------------------------
    # 4. Filter relationships by confidence
    # ------------------------------------------------------------------
    relationships = []
    rejected_count = 0

    for rel in raw_relationships:
        if not isinstance(rel, dict):
            continue

        raw_conf = rel.get("confidence", 0.0)
        confidence = float(raw_conf) if raw_conf is not None else 0.0

        if confidence < CONFIDENCE_THRESHOLD:
            logging.warning(
                f"⚠️ Rejected low-confidence relationship: "
                f"{rel.get('from_entity')} → {rel.get('to_entity')} "
                f"(confidence={confidence:.2f})"
            )
            observations.append(
                f"Relationship '{rel.get('from_entity')}' → '{rel.get('to_entity')}' "
                f"rejected: confidence {confidence:.2f} < threshold {CONFIDENCE_THRESHOLD}"
            )
            rejected_count += 1
            continue

        relationships.append({
            "from_table": _normalize_name(rel.get("from_entity", "")),
            "from_column": rel.get("from_column", ""),
            "to_table": _normalize_name(rel.get("to_entity", "")),
            "to_column": rel.get("to_column", ""),
            "relationship_type": rel.get("type", ""),
            "description":       rel.get("description", ""),
            "confidence":        confidence
        })

    
    # ------------------------------------------------------------------
    # 5. Log summary
    # ------------------------------------------------------------------
    logging.info("=" * 60)
    logging.info("✅ AI ER Model Transformation Complete:")
    logging.info(f"   - Entities:              {len(tables_info)}")
    logging.info(f"   - Relationships accepted: {len(relationships)}")
    logging.info(f"   - Relationships rejected: {rejected_count}")
    logging.info(f"   - Surrogate keys created: "
                 f"{sum(len(t['surrogate_keys']) for t in tables_info)}")
    for obs in observations:
        logging.info(f"   📝 {obs}")
    logging.info("=" * 60)

    # Drop relationships that reference entities not in tables_info
    defined_entities = {_normalize_name(t["table"]) for t in tables_info}
    filtered_relationships = []
    for rel in relationships:
        from_e = _normalize_name(rel.get("from_table", ""))
        to_e   = _normalize_name(rel.get("to_table", ""))
        if from_e not in defined_entities or to_e not in defined_entities:
            logging.warning(
                f"⚠️ Dropping relationship {from_e} → {to_e}: "
                f"entity not defined in normalized_entities"
            )
            observations.append(
                f"DROPPED_REL: {from_e} → {to_e} | "
                f"reason: entity not defined in normalized_entities"
            )
        else:
            filtered_relationships.append(rel)
    relationships = filtered_relationships

    return {
        "tables":              tables_info,
        "relationships":       relationships,
        "raw_entity_analysis": ai_result.get("raw_entity_analysis", {}),
        "cardinality_diagram": ai_result.get("cardinality_diagram", ""),
        "standalone_entities": ai_result.get("standalone_entities", []),
        "observations":        observations,
        "fact_tables":         [],
        "dimension_tables":    [],
        "model_source":        "ai",
        "ai_retry_available":  False
    }


def _find_source_file(col_name_lower: str, schemas: list, preferred_table: str = "") -> str:
    """Return the table_name that contains this column, or 'unknown'.
    Checks preferred_table (derived_from) first to avoid cross-table misattribution."""
    # First pass: only look in the preferred source table
    if preferred_table:
        for schema in schemas:
            if schema.get("table_name", "").lower() != preferred_table.lower():
                continue
            for col in schema.get("columns", []):
                if col.get("column_name", "").strip().lower() == col_name_lower:
                    return schema.get("table_name", "unknown")
    # Second pass: fallback to any matching schema
    for schema in schemas:
        for col in schema.get("columns", []):
            if col.get("column_name", "").strip().lower() == col_name_lower:
                return schema.get("table_name", "unknown")
    return "unknown"


def _passes_fact_gates(key, relationship_info, schema_map, inbound_refs, median_row_count):
    """Deterministic checks a fact-table candidate must pass, regardless of
    whether it was AI-designated or heuristic-scored."""
    entity = next(
        (e for e in relationship_info.get("tables", [])
         if _normalize_name(e.get("table", "")) == key),
        None
    )
    if not entity:
        return False

    source_schemas = _resolve_source_schemas(entity, schema_map) or (
        [schema_map[key]] if key in schema_map else []
    )
    if not source_schemas:
        return False

    if inbound_refs[key] >= 2:
        return False

    outgoing_fk_count = len(entity.get("foreign_keys", []))
    if outgoing_fk_count < 1:
        return False

    pk_set = set(_normalize_name(p) for p in entity.get("primary_keys", []))
    fk_set = set(_normalize_name(f["column"]) for f in entity.get("foreign_keys", []))
    numeric_types = {"int", "double", "float", "decimal", "bigint", "long"}

    measure_count = sum(
        1
        for source_schema in source_schemas
        for col in source_schema.get("columns", [])
        if _normalize_name(col["column_name"]) not in pk_set
        and _normalize_name(col["column_name"]) not in fk_set
        and any(t in col.get("data_type", "").lower() for t in numeric_types)
    )
    if measure_count < 1:
        return False

    row_count = sum(s.get("row_count", 0) for s in source_schemas)
    if row_count < median_row_count:
        return False

    return True

def verify_and_clean_model(relationship_info: dict, schemas: list, source_dfs: dict) -> dict:
    """
    Deterministic verification layer applied after AI transform, before saving.
    Validates PKs, FKs, relationships, fact selection, and entity names.
    Normalizes for comparison only — never mutates display fields.
    """
    observations = relationship_info.get("observations", [])
    logging.info("🔖 shared.py VERSION CHECK: patches A-E loaded")

    # ── Normalized lookup structures ─────────────────────────────
    schema_map  = {_normalize_name(s["table_name"]): s for s in schemas}
    source_norm = {_normalize_name(k): v for k, v in source_dfs.items()}

    # pk_owner: normalized PK column name -> normalized name of the entity
    # that "owns" it as a primary key. Built once, up front, from the AI's
    # raw (pre-validation) declarations so later checks aren't order-
    # dependent. Used by BLOCK 0 (does a dimension for this ID already
    # exist?) and BLOCK 1 (cross-entity PK conflict).
    #
    # Ownership prefers an entity where the column is a STANDALONE (single-
    # column) PK — that's the strongest signal of the column being that
    # table's own identity. A column that only ever appears as one part of
    # a composite key (e.g. a fact/junction table's [OrderID, CustomerID])
    # is a foreign-key reference to the real owner, not a competing identity.
    pk_owner = {}
    for t in relationship_info.get("tables", []):
        pks = t.get("primary_keys", [])
        if len(pks) == 1:
            pk_owner.setdefault(_normalize_name(pks[0]), _normalize_name(t.get("table", "")))
    for t in relationship_info.get("tables", []):
        t_key = _normalize_name(t.get("table", ""))
        for pk in t.get("primary_keys", []):
            pk_owner.setdefault(_normalize_name(pk), t_key)

    # ── BLOCK 0: Decomposition Consistency Check ──────────────────
    # Don't trust the AI's `is_denormalized` flag blindly — verify it against
    # actual column cardinality. A table that was left as a single entity but
    # contains 2+ low-cardinality, repeating ID-pattern columns (e.g.
    # MachineID, LineID, OperatorID each with far fewer distinct values than
    # rows) IS denormalized and must be decomposed into separate dimension
    # entities, regardless of what the AI said.
    raw_analysis = relationship_info.setdefault("raw_entity_analysis", {})

    derived_from_counts = Counter(
        _normalize_name(t.get("derived_from", ""))
        for t in relationship_info.get("tables", [])
        if t.get("derived_from")
    )

    synthesized_dims = []

    for entity in list(relationship_info.get("tables", [])):
        display       = entity.get("table", "")
        key           = _normalize_name(display)
        derived       = entity.get("derived_from", "")
        derived_norm  = _normalize_name(derived)
        source_schema = schema_map.get(derived_norm)

        if not source_schema or not derived_norm:
            continue

        # Only relevant for tables that were NOT already split into multiple
        # entities — a properly decomposed source will have count > 1.
        if derived_from_counts.get(derived_norm, 0) > 1:
            continue

        row_count = source_schema.get("row_count", 0)
        if row_count < MIN_ROWS_FOR_DECOMPOSITION_CHECK:
            continue

        own_pk_norm = {_normalize_name(p) for p in entity.get("primary_keys", [])}
        own_fk_norm = {_normalize_name(f["column"]) for f in entity.get("foreign_keys", [])}

        # Collect every unlinked ID-pattern column on this table, noting
        # which ones are also low-cardinality (repeat across rows).
        id_cols = []  # (cname, cnorm, data_type, is_low_card)
        for col in source_schema.get("columns", []):
            cname = col.get("column_name", "")
            cnorm = _normalize_name(cname)
            if not _is_id_pattern_column(cname, own_pk_norm):
                continue
            if cnorm in own_fk_norm:
                continue  # already wired up as an FK — not a gap
            distinct_count = col.get("distinct_count", 0)
            if distinct_count <= 0:
                continue
            is_low_card = (distinct_count / row_count) < ID_CARDINALITY_THRESHOLD
            id_cols.append((cname, cnorm, col.get("data_type", "string"), is_low_card))

        low_card_cols = [c for c in id_cols if c[3]]

        # AI-driven decomposition trigger
        raw_entry = raw_analysis.get(derived, {})
        ai_said_denormalized = raw_entry.get("is_denormalized", False)
        embedded_concepts = raw_entry.get("embedded_concepts", [])

        ai_flagged_low_card_cols = [
            c for c in low_card_cols
            if any(
                _normalize_name(concept) in c[1]
                for concept in embedded_concepts
            )
        ]

        if (
            len(low_card_cols) >= MIN_LOW_CARD_ID_COLS_FOR_DECOMPOSITION
            or (ai_said_denormalized and ai_flagged_low_card_cols)
        ):
            raw_entry = raw_analysis.setdefault(derived, {})
            cols_to_decompose = list({
                c[1]: c
                for c in (low_card_cols + ai_flagged_low_card_cols)
            }.values())
            if not raw_entry.get("is_denormalized", False):
                observations.append(
                    f"OVERRIDE: is_denormalized forced True for '{display.lower()}' — "
                    f"found {len(low_card_cols)} low-cardinality ID column(s) "
                    f"({', '.join(c[0] for c in low_card_cols)}) not reflected by AI flag"
                )
            raw_entry["is_denormalized"] = True

            # for cname, cnorm, data_type, _ in low_card_cols:
            for cname, cnorm, data_type, _ in cols_to_decompose:
                if pk_owner.get(cnorm) is not None:
                    continue  # a dimension already exists — sub-step B links it
                base = re.sub(r"id$", "", cname, flags=re.I).strip("_") or cname
                sibling_cols = [
                    c for c in source_schema.get("columns", [])
                    if _normalize_name(c.get("column_name", "")).startswith(_normalize_name(base))
                    and _normalize_name(c.get("column_name", "")) != cnorm
                ]
                dim_table_name = f"{base}_dimension"
                dim_entity = {
                    "table":          dim_table_name,
                    "derived_from":   derived,
                    "primary_keys":   [cname],
                    "surrogate_keys": [],
                    "source_columns": [cname],
                    "foreign_keys":   [],
                    "attributes": [
                        {
                            "name": cname,
                            "data_type": data_type,
                            "is_foreign_key": False,
                            "is_surrogate": False,
                            "references": None,
                            "source": derived,
                            "display_label": cname,
                            "tooltip": f"Source column from {derived}"
                        }
                    ]
                }
                synthesized_dims.append(dim_entity)
                moved = {
                    cnorm,
                    *[
                        _normalize_name(c["column_name"])
                        for c in sibling_cols
                    ]
                }

                entity["attributes"] = [
                    a for a in entity.get("attributes", [])
                    if _normalize_name(a.get("name", "")) not in moved
                ]

                entity["source_columns"] = [
                    c for c in entity.get("source_columns", [])
                    if _normalize_name(c) not in moved
                ]
                for sibling in sibling_cols:
                    dim_entity["attributes"].append({
                        "name": sibling["column_name"],
                        "data_type": sibling.get("data_type", "string"),
                        "is_foreign_key": False,
                        "is_surrogate": False,
                        "references": None,
                        "source": derived,
                        "display_label": sibling["column_name"],
                        "tooltip": f"Source column from {derived}"
                    })

                    dim_entity.setdefault("source_columns", []).append(
                        sibling["column_name"]
                    )
                pk_owner[cnorm] = _normalize_name(dim_table_name)
                observations.append(
                    f"CREATED_DIM: {dim_table_name.lower()} | "
                    f"reason: decomposition consistency — '{cname}' is a low-cardinality "
                    f"ID column on '{display.lower()}' with no dimension of its own"
                )

        # ---- Sub-step B: connectivity — link ANY unlinked ID-pattern column
        # (low-cardinality or not, whether or not sub-step A fired this pass)
        # to a dimension that already owns it as PK, e.g. one the AI itself
        # created, or one sub-step A just synthesized above. No "2+" gate
        # here: even a single unlinked column must not be left dangling if
        # a matching dimension exists.
        for cname, cnorm, data_type, _ in id_cols:
            dim_owner = pk_owner.get(cnorm)
            if dim_owner is None or dim_owner == key:
                continue  # no dimension owns this column elsewhere
            if cnorm in own_fk_norm:
                continue  # already linked

            entity.setdefault("foreign_keys", []).append({
                "column":            cname,
                "references_table":  dim_owner,
                "references_column": cname
            })
            own_fk_norm.add(cnorm)

            already_linked = any(
                _normalize_name(r.get("from_table", "")) == _normalize_name(display)
                and _normalize_name(r.get("from_column", "")) == cnorm
                and _normalize_name(r.get("to_table", "")) == dim_owner
                for r in relationship_info.get("relationships", [])
            )
            if not already_linked:
                relationship_info.setdefault("relationships", []).append({
                    "from_table":         _normalize_name(display),
                    "from_column":        cname,
                    "to_table":           dim_owner,
                    "to_column":          cname,
                    "relationship_type":  "1:M",
                    "description":        "Auto-linked: matching dimension existed but FK was missing",
                    "confidence":         1.0
                })
            observations.append(
                f"AUTO_LINKED_FK: {display.lower()}.{cname} → {dim_owner}.{cname} | "
                f"reason: matching dimension existed but was never wired as a foreign key"
            )

    relationship_info["tables"].extend(synthesized_dims)

    # ── BLOCK 1: PK Validation ───────────────────────────────────
    invalid_pk_targets = set()  # normalized names of tables with no valid PK after validation

    for entity in relationship_info.get("tables", []):
        display      = entity.get("table", "")
        display_obs = display.lower()
        
        key          = _normalize_name(display)
        derived_norm = _normalize_name(entity.get("derived_from", ""))
        source_schema = schema_map.get(derived_norm) or schema_map.get(key)

        raw_analysis = relationship_info.get("raw_entity_analysis", {})
        # find the raw entry for this entity's source
        raw_entry = raw_analysis.get(entity.get("derived_from", ""), {})
        is_denormalized = raw_entry.get("is_denormalized", False)

        if not source_schema:
            continue

        row_count   = source_schema.get("row_count", 0)
        col_lookup  = {
            _normalize_name(c["column_name"]): c
            for c in source_schema.get("columns", [])
        }

        validated_pks = []

        for pk in entity.get("primary_keys", []):
            pk_norm = _normalize_name(pk)
            surrogate_set = {_normalize_name(s) for s in entity.get("surrogate_keys", [])}
            if pk_norm in surrogate_set:
                validated_pks.append(pk)
                continue

            # Check -1: cross-entity PK conflict — if this column name is
            # already the declared PK of a DIFFERENT entity in the same
            # model, it's a foreign key here, not this table's own identity,
            # even if it happens to be unique in both places.
            owner = pk_owner.get(pk_norm)

            if owner is not None and owner != key:

                is_composite = len(entity.get("primary_keys", [])) > 1

                if not is_composite:
                    observations.append(
                        f"REJECTED_PK: {display_obs}.{pk} | reason: '{pk}' is already the "
                        f"declared primary key of entity '{owner}' — treating as a foreign "
                        f"key reference here, not this table's own identity"
                    )
                    continue

                other_pks = [
                    p for p in entity.get("primary_keys", [])
                    if _normalize_name(p) != pk_norm
                ]

                keep_pk = True

                df = source_norm.get(derived_norm)

                if df is not None:
                    try:
                        lookup = {c.lower(): c for c in df.columns}

                        cols = [
                            lookup[p.lower()]
                            for p in other_pks
                            if p.lower() in lookup
                        ]

                        if len(cols) == len(other_pks) and cols:
                            other_distinct = len(df[cols].drop_duplicates())
                            keep_pk = other_distinct < len(df)
                    except Exception:
                        keep_pk = True

                if keep_pk:
                    validated_pks.append(pk)
                    continue

                observations.append(
                    f"REJECTED_PK: {display_obs}.{pk} | reason: '{pk}' is already the "
                    f"declared primary key of entity '{owner}'"
                )
                continue
            col     = col_lookup.get(pk_norm)
            if not col:
                observations.append(
                    f"REJECTED_PK: {display_obs}.{pk} | reason: column not found in source schema"
                )
                continue

            # Check 0: reject multi-valued columns
            sample_vals = col.get("sample_values", [])
            if any(
                isinstance(v, str) and ("|" in v or ("," in v and len(v.split(",")) > 2))
                for v in sample_vals
            ):
                observations.append(
                    f"REJECTED_PK: {display_obs}.{pk} | reason: multi-valued column (contains separators)"
                )
                continue

            null_pct       = col.get("null_percentage", 100.0)
            distinct_count = col.get("distinct_count", 0)
            data_type      = col.get("data_type", "").lower()

            # Check 1: nulls
            if null_pct > 0:
                observations.append(
                    f"REJECTED_PK: {display_obs}.{pk} | reason: null_percentage={null_pct}% > 0%"
                )
                continue

            # Check 2: distinctness
            if not is_denormalized and row_count > 0 and (distinct_count / row_count) < 0.98:
                observations.append(
                    f"REJECTED_PK: {display_obs}.{pk} | "
                    f"reason: distinct_ratio={distinct_count}/{row_count} < 0.98"
                )
                continue

            # Check 3: float/double — allow if sample values are integer-like
            if "float" in data_type or "double" in data_type:
                sample_vals = col.get("sample_values", [])
                all_integer_like = all(
                    str(v).replace(".0", "").isdigit()
                    for v in sample_vals if v is not None
                )
                if not all_integer_like:
                    observations.append(
                        f"REJECTED_PK: {display_obs}.{pk} | "
                        f"reason: data_type is {data_type} with non-integer values"
                    )
                    continue

            validated_pks.append(pk)

        entity["primary_keys"] = validated_pks

        if not validated_pks and not entity.get("surrogate_keys"):
            invalid_pk_targets.add(key)

        if not validated_pks and entity.get("surrogate_keys"):
            # Promote first surrogate to PK
            promoted = entity["surrogate_keys"][0]
            entity["primary_keys"] = [promoted]
            invalid_pk_targets.discard(key)
            observations.append(
                f"INFO: {display_obs} — promoted surrogate '{promoted}' to primary key"
            )
    # ── BLOCK 2: FK + Relationship Validation ────────────────────
    cleaned_relationships = []

    for rel in relationship_info.get("relationships", []):
        from_table  = rel.get("from_table", "")
        from_obs    = from_table.lower()
        to_table    = rel.get("to_table", "")
        to_obs      = to_table.lower()
        from_col    = rel.get("from_column", "")
        to_col      = rel.get("to_column", "")
        from_norm   = _normalize_name(from_table)
        to_norm     = _normalize_name(to_table)

        reject_reason = None

        # Gate: target table must have a valid PK
        if to_norm in invalid_pk_targets:
            reject_reason = f"target table '{to_table}' has no valid primary key"

        # Check A: type compatibility
        if not reject_reason:
            from_schema = schema_map.get(from_norm)
            to_schema   = schema_map.get(to_norm)

            if from_schema and to_schema:
                from_col_lookup = {
                    _normalize_name(c["column_name"]): c
                    for c in from_schema.get("columns", [])
                }
                to_col_lookup = {
                    _normalize_name(c["column_name"]): c
                    for c in to_schema.get("columns", [])
                }

                from_col_meta = from_col_lookup.get(_normalize_name(from_col))
                to_col_meta   = to_col_lookup.get(_normalize_name(to_col))

                if from_col_meta and to_col_meta:
                    from_type = from_col_meta.get("data_type", "").lower()
                    to_type   = to_col_meta.get("data_type", "").lower()

                    numeric = {"int", "double", "float", "bigint", "long", "decimal"}
                    string  = {"string", "varchar", "nvarchar", "text", "char"}

                    from_is_numeric = any(t in from_type for t in numeric)
                    from_is_string  = any(t in from_type for t in string)
                    to_is_numeric   = any(t in to_type for t in numeric)
                    to_is_string    = any(t in to_type for t in string)

                    if not _datatypes_compatible(from_type, to_type):
                        reject_reason = f"type_mismatch ({from_type} vs {to_type})"

                # Check C: cardinality sanity
                if not reject_reason and from_col_meta and to_schema:
                    child_distinct  = from_col_meta.get("distinct_count", 0)
                    parent_row_count = to_schema.get("row_count", 0)
                    if parent_row_count > 0 and child_distinct > parent_row_count * 2:
                        reject_reason = (
                            f"cardinality_impossible: child_distinct={child_distinct} > "
                            f"parent_rows={parent_row_count} * 2"
                        )

        # Check B: referential sampling (only when DataFrames available)
        if not reject_reason:
            child_df  = source_norm.get(from_norm)
            parent_df = source_norm.get(to_norm)

            if child_df is not None and parent_df is not None:
                try:
                    child_cols = {c.lower(): c for c in child_df.columns}
                    parent_cols = {c.lower(): c for c in parent_df.columns}

                    if from_col.lower() in child_cols and to_col.lower() in parent_cols:
                        child_vals = child_df[child_cols[from_col.lower()]].dropna().unique()[:1000]
                        parent_set = set(parent_df[parent_cols[to_col.lower()]].dropna().unique())
                        if len(child_vals) > 0:
                            match_rate = sum(1 for v in child_vals if v in parent_set) / len(child_vals)
                            if match_rate < 0.95:
                                reject_reason = (
                                    f"low_match_rate={match_rate:.2f} < 0.95"
                                )
                except Exception as e:
                    logging.warning(f"Referential check skipped for {from_table}.{from_col}: {e}")

        if reject_reason:
            observations.append(
                f"REJECTED_FK: {from_obs}.{from_col} → {to_obs}.{to_col} "
                f"| reason: {reject_reason}"
            )
        else:
            cleaned_relationships.append(rel)   # original object, not mutated

    relationship_info["relationships"] = cleaned_relationships

    # ── BLOCK 3: Fact Table Validation + Override ─────────────────
    inbound_refs = Counter(
        _normalize_name(r.get("to_table", ""))
        for r in cleaned_relationships
    )

    all_row_counts  = []

    for entity in relationship_info.get("tables", []):
        derived_norm  = _normalize_name(entity.get("derived_from", ""))
        source_schema = schema_map.get(derived_norm)
        if source_schema:
            all_row_counts.append(source_schema.get("row_count", 0))

    if not all_row_counts:
        median_row_count = 0
    elif len(all_row_counts) == 1:
        # Single table: set median to 0 so the row count gate always passes
        median_row_count = 0
    else:
        sorted_counts = sorted(all_row_counts)
        mid = len(sorted_counts) // 2
        median_row_count = (
            (sorted_counts[mid - 1] + sorted_counts[mid]) / 2
            if len(sorted_counts) % 2 == 0
            else sorted_counts[mid]
        )
    ai_designated_fact = next(
        (
            e for e in relationship_info.get("tables", [])
            if _normalize_name(e.get("table", "")) == "fact_table"
        ),
        None,
    )

    best_candidate = None

    if ai_designated_fact:
        candidate_key = _normalize_name(ai_designated_fact.get("table", ""))

        if _passes_fact_gates(
            candidate_key,
            relationship_info,
            schema_map,
            inbound_refs,
            median_row_count,
        ):
            best_candidate = candidate_key
            logging.info(f"   ✅ AI fact designation validated: {best_candidate}")
        else:
            logging.warning(
                f"   ⚠️ AI fact designation '{candidate_key}' failed validation gates — falling back to heuristic scoring"
            )

    if best_candidate is None:
        best_score = -1

        for entity in relationship_info.get("tables", []):
            display = entity.get("table", "")
            key = _normalize_name(display)
            source_schemas = _resolve_source_schemas(entity, schema_map) or (
                [schema_map[key]] if key in schema_map else []
            )
            if not source_schemas:
                continue

            outgoing_fk_count = len(entity.get("foreign_keys", []))
            row_count = source_schema.get("row_count", 0)

            if inbound_refs[key] >= 2:
                continue

            pk_set = {_normalize_name(p) for p in entity.get("primary_keys", [])}
            fk_set = {_normalize_name(f["column"]) for f in entity.get("foreign_keys", [])}
            numeric_types = {"int", "double", "float", "decimal", "bigint", "long"}

            measure_count = sum(
                1
                for col in source_schema.get("columns", [])
                if _normalize_name(col["column_name"]) not in pk_set
                and _normalize_name(col["column_name"]) not in fk_set
                and any(t in col.get("data_type", "").lower() for t in numeric_types)
            )

            if outgoing_fk_count < 1:
                continue
            if measure_count < 1:
                continue
            if row_count < median_row_count:
                continue

            row_count_weight = row_count / (median_row_count or 1)
            score = outgoing_fk_count * 3 + measure_count * 4 + row_count_weight

            if score > best_score:
                best_score = score
                best_candidate = key

    relationship_info["fact_entity_override"] = best_candidate

    # ── BLOCK 3a: Fact Table Connectivity Safety Net ────────────────
    # A fact table should end up with FKs pointing to essentially every
    # dimension it's meaningfully connected to. BLOCK 0 already fixes the
    # common cause (a dimension that was never created), but if a matching
    # dimension DOES exist and the AI simply forgot to wire the FK, catch
    # that gap here too.
    if best_candidate:
        fact_entity_for_link = next(
            (e for e in relationship_info["tables"]
             if _normalize_name(e.get("table", "")) == best_candidate),
            None
        )
        if fact_entity_for_link:
            fact_derived_for_link = _normalize_name(fact_entity_for_link.get("derived_from", ""))
            fact_source_schema = schema_map.get(fact_derived_for_link)
            fact_pk_norm = {_normalize_name(p) for p in fact_entity_for_link.get("primary_keys", [])}
            fact_fk_norm = {_normalize_name(f["column"]) for f in fact_entity_for_link.get("foreign_keys", [])}

            if fact_source_schema:
                for col in fact_source_schema.get("columns", []):
                    cname = col.get("column_name", "")
                    cnorm = _normalize_name(cname)
                    if not _is_id_pattern_column(cname, fact_pk_norm):
                        continue
                    if cnorm in fact_fk_norm:
                        continue  # already linked

                    dim_owner = pk_owner.get(cnorm)
                    if not dim_owner or dim_owner == best_candidate:
                        continue  # no matching dimension exists — not this block's job

                    fact_entity_for_link.setdefault("foreign_keys", []).append({
                        "column":            cname,
                        "references_table":  dim_owner,
                        "references_column": cname
                    })
                    fact_fk_norm.add(cnorm)

                    already_linked = any(
                        _normalize_name(r.get("from_table", "")) == best_candidate
                        and _normalize_name(r.get("from_column", "")) == cnorm
                        and _normalize_name(r.get("to_table", "")) == dim_owner
                        for r in relationship_info.get("relationships", [])
                    )
                    if not already_linked:
                        relationship_info.setdefault("relationships", []).append({
                            "from_table":         best_candidate,
                            "from_column":        cname,
                            "to_table":           dim_owner,
                            "to_column":          cname,
                            "relationship_type":  "1:M",
                            "description":        "Auto-linked: matching dimension existed but FK was missing",
                            "confidence":         1.0
                        })
                    observations.append(
                        f"AUTO_LINKED_FK: {best_candidate}.{cname} → {dim_owner}.{cname} | "
                        f"reason: matching dimension existed but was never wired as a foreign key"
                    )

    # ── BLOCK 3b: Remove ghost DIM that duplicates the fact grain ────
    if best_candidate:
        fact_entity = next(
            (e for e in relationship_info["tables"]
            if _normalize_name(e.get("table", "")) == best_candidate),
            None
        )
        if fact_entity:
            fact_derived = _normalize_name(fact_entity.get("derived_from", ""))
            fact_fk_set  = {_normalize_name(f["column"]) for f in fact_entity.get("foreign_keys", [])}

            ghost_keys = set()

            # Guard: if derived_from is empty, skip ghost removal entirely
            # to avoid accidentally matching all entities with missing derived_from
            if not fact_derived:
                logging.warning(
                    f"fact_entity '{best_candidate}' has no derived_from — "
                    f"skipping ghost DIM removal to avoid false positives"
                )
            else:
                for entity in relationship_info["tables"]:
                    if _normalize_name(entity.get("table", "")) == best_candidate:
                        continue
                    entity_derived = _normalize_name(entity.get("derived_from", ""))
                    if not entity_derived or entity_derived != fact_derived:
                        continue
                    entity_fk_set = {_normalize_name(f["column"]) for f in entity.get("foreign_keys", [])}
                    if not fact_fk_set:
                        continue
                    overlap = len(fact_fk_set & entity_fk_set) / len(fact_fk_set)
                    if overlap >= 0.7:
                        ghost_keys.add(_normalize_name(entity.get("table", "")))
                        observations.append(
                            f"REMOVED_GHOST_DIM: {entity.get('table', '').lower()} | "
                            f"reason: duplicate of fact grain '{best_candidate}' "
                            f"({overlap:.0%} FK overlap, same derived_from)"
                        )

                relationship_info["tables"] = [
                    e for e in relationship_info["tables"]
                    if _normalize_name(e.get("table", "")) not in ghost_keys
                ]

    if best_candidate is None:
        observations.append(
            "WARNING: No valid fact table candidate found. Model returned as ER-only."
        )
    else:
        logging.info(f"   ✅ Fact entity override: {best_candidate}")

    # ── BLOCK 3c: Standalone Entity Extraction ─────────────────────
    # Any entity that never appears as from_table/to_table in the final
    # relationship list — and isn't the fact table itself — gets pulled
    # out into standalone_entities instead of being left as a fake dim.
    connected_keys = set()
    for r in relationship_info.get("relationships", []):
        connected_keys.add(_normalize_name(r.get("from_table", "")))
        connected_keys.add(_normalize_name(r.get("to_table", "")))

    standalone_tables = []
    remaining_tables  = []
    for entity in relationship_info.get("tables", []):
        key = _normalize_name(entity.get("table", ""))
        if key == best_candidate or key in connected_keys:
            remaining_tables.append(entity)
        else:
            standalone_tables.append(entity)
            observations.append(
                f"STANDALONE_ENTITY: {entity.get('table', '').lower()} | "
                f"reason: no relationship to any other entity in the model"
            )

    relationship_info["tables"] = remaining_tables
    relationship_info.setdefault("standalone_entities", [])
    relationship_info["standalone_entities"].extend(
        e.get("table", "") for e in standalone_tables
    )
    # keep full definitions too, not just names, so DDL/output generation
    # can still render these tables on their own
    # Any name that was merged into another entity (e.g. fact_table via
    # Rule 9b) is not actually standalone — it has a relationship, it's
    # just not expressed as a from/to pair. Strip it out.
    merged_source_names = set()
    for entity in relationship_info.get("tables", []):
        merged_source_names.update(_derived_from_list(entity))

    relationship_info["standalone_entities"] = [
        name for name in relationship_info.get("standalone_entities", [])
        if _normalize_name(name) not in merged_source_names
    ]
    relationship_info["standalone_entity_definitions"] = [
        e for e in relationship_info.get("standalone_entity_definitions", [])
        if _normalize_name(e.get("table", "")) not in merged_source_names
    ]
   

    # ── BLOCK 4: Entity Name Guard ────────────────────────────────
    valid_tables = []
    for entity in relationship_info.get("tables", []):
        display      = entity.get("table", "")
        display_obs  = display.lower()
        key          = _normalize_name(display)
        derived_norm = _normalize_name(entity.get("derived_from", ""))
        logging.info(f"   BLOCK4 CHECK: {display_obs} | derived_norm={derived_norm} | in schema_map={derived_norm in schema_map} | schema_map_keys={list(schema_map.keys())}")


        derived_parts = _derived_from_list(entity)
        if derived_parts:
            matched = [d for d in derived_parts if d in schema_map]
            if not matched:
                observations.append(
                    f"REJECTED_ENTITY: {display_obs} | "
                    f"derived_from '{entity.get('derived_from')}' not traceable to any source schema"
                )
                continue

        valid_tables.append(entity)   # original object

    relationship_info["tables"]       = valid_tables
    # ── PATCH E: Orphan Relationship Cleanup ───────────────────────
    surviving_keys = {_normalize_name(e.get("table", "")) for e in valid_tables}
    kept_relationships = []
    logging.info(f"🧹 PATCH_E_ACTIVE | relationships_checked={len(relationship_info.get('relationships', []))} | dropped={len(relationship_info.get('relationships', [])) - len(kept_relationships)}")
    for r in relationship_info.get("relationships", []):
        from_key = _normalize_name(r.get("from_table", ""))
        to_key   = _normalize_name(r.get("to_table", ""))
        if from_key in surviving_keys and to_key in surviving_keys:
            kept_relationships.append(r)
        else:
            observations.append(
                f"REJECTED_RELATIONSHIP: {r.get('from_table','')}→{r.get('to_table','')} "
                f"| reason: references entity dropped in BLOCK 4"
            )
    relationship_info["relationships"] = kept_relationships
    relationship_info["observations"] = observations
    return relationship_info

# ====================================================================
# RULE-BASED FALLBACK
# ====================================================================

def _camel_to_snake(name: str) -> str:
    """MachineID -> machine_id, ProductionID -> production_id"""
    s1 = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
    s2 = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s1)
    return s2.lower()

    
def _fallback_relationship_detection(schemas):
    """
    Rule-based fallback when AI fails or returns invalid JSON.
    Does NOT attempt normalization — returns raw table structure.
    Sets model_source = "rule_based_fallback" and ai_retry_available = True
    so the frontend can offer the user a retry button.
    """
    logging.info("⚙️ Running rule-based fallback relationship detection")

    relationships = []
    tables_info   = []

    table_columns         = {}
    original_to_normalized = {}

    for schema in schemas:
        original_name  = schema.get("table_name", "").strip()
        normalized_name = _normalize_table_name(original_name)
        columns         = schema.get("columns", [])

        if normalized_name and columns:
            table_columns[normalized_name]          = columns
            original_to_normalized[normalized_name] = original_name
            logging.info(f"📋 Mapped: '{original_name}' → '{normalized_name}'")

    if not table_columns:
        logging.warning("⚠️ No valid tables found in fallback")
        return {
            "tables":             [],
            "relationships":      [],
            "model_source":       "rule_based_fallback",
            "ai_retry_available": True,
            "observations":       ["No valid tables found during fallback detection."]
        }

    all_table_names = list(table_columns.keys())

    # Step 1: Identify primary keys (with null guard)
    pk_map = {}
    for table_name, columns in table_columns.items():
        for col in columns:
            raw_name     = col.get("column_name", "").strip()
            col_name     = raw_name.lower()
            col_snake    = _camel_to_snake(raw_name)
            is_potential = col.get("is_potential_key", False)
            null_pct     = col.get("null_percentage", 100.0)

            if null_pct > NULL_THRESHOLD_FOR_KEY:
                continue

            if is_potential:
                if col_snake.endswith("_id"):
                    base = col_snake[:-3]
                    if base == table_name or base in table_name or table_name in base:
                        pk_map[table_name] = col_name
                        logging.info(f"✓ PK (fallback): {table_name}.{col_name}")
                        break
                elif col_name == "id":
                    pk_map[table_name] = col_name
                    logging.info(f"✓ PK (fallback): {table_name}.{col_name}")
                    break

    logging.info(f"📊 Fallback found {len(pk_map)} primary keys")

    # Step 2: Detect foreign keys
    for table_name, columns in table_columns.items():
        primary_keys = [pk_map[table_name]] if pk_map.get(table_name) else []
        foreign_keys = []

        for col in columns:
            raw_name  = col.get("column_name", "").strip()
            col_name  = raw_name.lower()
            col_snake = _camel_to_snake(raw_name)
            null_pct  = col.get("null_percentage", 100.0)

            if col_name == pk_map.get(table_name):
                continue
            if null_pct > NULL_THRESHOLD_FOR_KEY:
                continue

            if col_snake.endswith("_id"):
                referenced_table = _find_matching_table(col_snake, all_table_names)
                if referenced_table and referenced_table != table_name:
                    referenced_pk = pk_map.get(referenced_table)
                    if referenced_pk:
                        foreign_keys.append({
                            "column":            col_name,
                            "references_table":  referenced_table,
                            "references_column": referenced_pk
                        })
                        relationships.append({
                            "from_table":        table_name,
                            "from_column":       col_name,
                            "to_table":          referenced_table,
                            "to_column":         referenced_pk,
                            "relationship_type": "M:1",
                            "description":       f"Many {table_name} to One {referenced_table}",
                            "confidence":        None
                        })
                        logging.info(f"🔗 Fallback relationship: {table_name}.{col_name} → {referenced_table}.{referenced_pk}")
        original_name = original_to_normalized.get(table_name, table_name)

        # In fallback all columns are treated as source columns (no normalization)
        source_columns_list = [
            col.get("column_name", "")
            for col in columns
            if col.get("null_percentage", 100.0) <= NULL_THRESHOLD_FOR_KEY
            or col.get("column_name", "") in primary_keys
        ]

        tables_info.append({
            "table":          original_name,
            "derived_from":   original_name,
            "primary_keys":   primary_keys,
            "surrogate_keys": [],           # Fallback never creates surrogates
            "source_columns": source_columns_list,
            "foreign_keys":   foreign_keys,
            "attributes": [
                {
                    "name":          col.get("column_name", ""),
                    "data_type":     col.get("data_type", ""),
                    "is_foreign_key": col.get("column_name", "").lower() in
                                      {fk["column"] for fk in foreign_keys},
                    "is_surrogate":  False,
                    "source":        original_name,
                    "display_label": col.get("column_name", ""),
                    "tooltip":       f"Source column from {original_name}",
                    "references":    None
                }
                for col in columns
            ]
        })

    logging.info("=" * 60)
    logging.info("✅ Rule-based fallback complete:")
    logging.info(f"   - Tables:        {len(tables_info)}")
    logging.info(f"   - Relationships: {len(relationships)}")
    logging.info("=" * 60)

    return {
        "tables":              tables_info,
        "relationships":       relationships,
        "raw_entity_analysis": {},
        "cardinality_diagram": "",
        "standalone_entities": [],
        "observations":        [
            "AI modeling failed. Showing raw table structure without normalization.",
            "Use the Retry AI Modeling button to attempt AI-based ER generation again."
        ],
        "fact_tables":         [t["table"] for t in tables_info if t["foreign_keys"]],
        "dimension_tables":    [
            t["table"] for t in tables_info
            if not t["foreign_keys"] and t["primary_keys"]
        ],
        "model_source":        "rule_based_fallback",
        "ai_retry_available":  True
    }


# ====================================================================
# TABLE NAME HELPERS
# ====================================================================
def _normalize_table_name(table_name):
    """Normalize table names by removing common suffixes and standardizing format."""
    name = table_name.lower().strip()
    for suffix in [" - copy", "_copy", " copy", "_backup", " backup"]:
        if name.endswith(suffix):
            name = name[:-len(suffix)].strip()
    name = re.sub(r"[^a-z0-9_]", "", name)
    return name


def _find_matching_table(column_name, table_names):
    """Find best matching table for a foreign key column. column_name must already be snake_case (e.g. 'machine_id')."""
    if not column_name.endswith("_id"):
        return None

    base_name = column_name[:-3]

    if base_name in table_names:
        return base_name

    for pattern in [base_name + "s", base_name + "es", base_name[:-1] + "ies"]:
        if pattern in table_names:
            return pattern

    if base_name.endswith("s"):
        singular = base_name[:-1]
        if singular in table_names:
            return singular

    for table in table_names:
        if base_name in table or table in base_name:
            return table

    return None

# ====================================================================
# JSON EXTRACTION (Robust AI Cleanup)
# ====================================================================
def _extract_json_from_text(text):
    """Extracts JSON from noisy AI output. Raises ValueError if nothing found."""
    text = text.strip()

    # Try direct parse first
    try:
        return json.loads(text)
    except Exception:
        pass

    # Strip markdown fences
    cleaned = re.sub(r"```json", "", text, flags=re.IGNORECASE)
    cleaned = re.sub(r"```", "", cleaned).strip()
    try:
        return json.loads(cleaned)
    except Exception:
        pass

    # Find the largest JSON object in the text
    start = text.find("{")
    end   = text.rfind("}")
    if start == -1 or end == -1:
        raise ValueError("No JSON object found")

    candidate = text[start:end+1]

    # Repair pattern: AI sometimes emits invalid JS/Python-style string
    # concatenation, e.g.  "foo"\n+ "bar"  instead of a single string.
    # Collapse adjacent quoted strings joined by '+' into one string.
    repaired = re.sub(
        r'"\s*\n?\s*\+\s*\n?\s*"',
        '',
        candidate,
    )
    if repaired != candidate:
        try:
            result = json.loads(repaired)
            logging.warning(
                "⚠️ AI JSON had string-concatenation syntax ('+' between "
                "quoted strings) — repaired automatically before parsing."
            )
            return result
        except json.JSONDecodeError:
            pass  # repair didn't fix it — fall through to normal error path

    try:
        return json.loads(candidate)
    except json.JSONDecodeError as json_err:
        logging.error(
            f"❌ AI returned malformed JSON that could not be repaired: {json_err}. "
            f"Triggering fallback instead of saving corrupt model."
        )
        raise ValueError(
            f"AI JSON malformed and unrecoverable: {json_err}"
        ) from json_err

# ====================================================================
# DATATYPE HELPERS
# ====================================================================
def _datatypes_compatible(d1, d2):
    """Basic datatype compatibility check."""
    numeric = {"int","bigint","long","float","double","decimal"}
    string  = {"string","varchar","nvarchar","text","char"}

    d1 = d1.lower()
    d2 = d2.lower()

    if any(t in d1 for t in numeric) and any(t in d2 for t in numeric):
        return True
    if any(t in d1 for t in string) and any(t in d2 for t in string):
        return True
    return False


# ====================================================================
# SCHEMA EXTRACTION
# ====================================================================
def extract_schema_from_json_file(data_bytes, file_name, file_path):
    """Extract schema from JSON file with proper nested structure handling."""
    try:
        content = data_bytes.decode("utf-8")
        data    = json.loads(content)

        if isinstance(data, dict):
            # Find the largest array in the dict, not just the first one
            best_key = None
            best_len = 0
            for key, value in data.items():
                if isinstance(value, list) and len(value) > best_len:
                    best_key = key
                    best_len = len(value)

            if best_key is not None:
                logging.info(f"📦 Found largest nested array '{best_key}' with {best_len} records")
                df = pd.json_normalize(data[best_key], max_level=0)
                return extract_schema_metadata(df, best_key, file_path)
            else:
                logging.info("📄 Found single JSON object")
                df         = pd.json_normalize([data], max_level=0)
                table_name = file_name.rsplit(".", 1)[0]
                return extract_schema_metadata(df, table_name, file_path)

        elif isinstance(data, list):
            logging.info(f"📦 Found JSON array with {len(data)} records")
            df         = pd.json_normalize(data, max_level=0)
            table_name = file_name.rsplit(".", 1)[0]
            return extract_schema_metadata(df, table_name, file_path)

        else:
            logging.error(f"❌ Unsupported JSON structure in {file_name}")
            return None

    except json.JSONDecodeError as e:
        logging.error(f"❌ JSON parse error: {e}")
        return None
    except Exception as e:
        logging.error(f"❌ Error processing JSON: {e}")
        return None


def infer_enhanced_datatype(series, column_name):
    """Enhanced datatype inference including arrays and structs."""
    non_null = series.dropna()
    if len(non_null) == 0:
        return "string"

    first_value = non_null.iloc[0]

    if isinstance(first_value, list):
        if len(first_value) > 0:
            first_elem = first_value[0]
            if isinstance(first_elem, dict):
                fields = [f"{k} {infer_simple_type(v)}" for k, v in first_elem.items()]
                return f"array<struct<{', '.join(fields)}>>"
            else:
                return f"array<{infer_simple_type(first_elem)}>"
        return "array<string>"

    elif isinstance(first_value, dict):
        fields = [f"{k} {infer_simple_type(v)}" for k, v in first_value.items()]
        return f"struct<{', '.join(fields)}>"

    raw_dtype = str(series.dtype).lower()
    if "int"      in raw_dtype: return "int"
    if "float"    in raw_dtype or "double" in raw_dtype: return "double"
    if "bool"     in raw_dtype: return "boolean"
    if "datetime" in raw_dtype: return "date"
    return "string"


def infer_simple_type(value):
    """Helper to infer simple types for nested structures."""
    if isinstance(value, bool):  return "boolean"
    if isinstance(value, int):   return "int"
    if isinstance(value, float): return "double"
    if isinstance(value, list):  return "array"
    if isinstance(value, dict):  return "struct"
    return "string"

def _sanitize_col_name(col):
    clean = col.strip()
    clean = re.sub(r'[ ,;{}()\n\t=]+', '_', clean)
    clean = re.sub(r'_+', '_', clean)
    return clean.strip('_')


def extract_schema_metadata(df, table_name, file_path):
    """
    Extract comprehensive schema metadata from a pandas DataFrame.
    Includes null guard on is_potential_key:
      - Columns with null_percentage > NULL_THRESHOLD_FOR_KEY are NEVER keys.
    """
    total_rows      = len(df)
    table_name_clean = (
        table_name.rsplit(".", 1)[0] if "." in table_name else table_name
    ).lower().strip().replace(" ", "_").replace("-", "_")

    schema_info = {
        "table_name":           table_name_clean,
        "file_path":            file_path,
        "row_count":            int(total_rows),
        "column_count":         int(len(df.columns)),
        "extraction_timestamp": datetime.utcnow().isoformat(),
        "columns":              []
    }

    # Rename DataFrame columns first
    df = df.rename(columns={col: _sanitize_col_name(col) for col in df.columns})

    for column in df.columns:
        series = df[column]
        dtype  = infer_enhanced_datatype(series, column)

        series_for_stats = series.copy()
        if series_for_stats.apply(lambda x: isinstance(x, (dict, list))).any():
            series_for_stats = series_for_stats.apply(
                lambda x: json.dumps(x, sort_keys=True) if isinstance(x, (dict, list)) else x
            )

        null_count = int(series_for_stats.isna().sum())
        null_pct   = float(round((null_count / total_rows) * 100, 2)) if total_rows else 0.0

        try:
            distinct_count = int(series_for_stats.nunique())
        except Exception:
            try:
                distinct_count = len(set(str(x) for x in series_for_stats.dropna().tolist()))
            except Exception:
                distinct_count = 0

        is_nullable         = bool(series_for_stats.isna().any())
        cardinality_pct     = float(round((distinct_count / total_rows) * 100, 2)) if total_rows else 0.0

        col_info = {
            "column_name":          str(column),
            "data_type":            dtype,
            "nullable":             is_nullable,
            "null_count":           int(null_count),
            "null_percentage":      null_pct,
            "distinct_count":       int(distinct_count),
            "cardinality_percentage": cardinality_pct,
            "is_potential_key":     False
        }

        # Key detection — HARD NULL GUARD FIRST
        # A column with > NULL_THRESHOLD_FOR_KEY % nulls is never a key, period.
        if null_pct <= NULL_THRESHOLD_FOR_KEY:
            if cardinality_pct == 100.0 and null_count == 0:
                if "id" in str(column).lower():
                    col_info["is_potential_key"] = True
            elif cardinality_pct > 95 and total_rows > 100:
                if "id" in str(column).lower() or "key" in str(column).lower():
                    col_info["is_potential_key"] = True
        else:
            logging.debug(
                f"   ⛔ '{column}' excluded from key inference "
                f"(null_percentage={null_pct}% > {NULL_THRESHOLD_FOR_KEY}%)"
            )

        if not dtype.startswith(("array", "struct")):
            try:
                sample_vals = series_for_stats.dropna().unique()[:5].tolist()
                col_info["sample_values"] = [str(v) for v in sample_vals]
            except Exception:
                col_info["sample_values"] = []
        else:
            col_info["sample_values"] = []

        schema_info["columns"].append(col_info)

    return schema_info


# ====================================================================
# BATCH SCHEMA LOADER
# ====================================================================
def load_batch_schemas():
    """Loads ONLY schema files created after batch_start timestamp."""
    account_name = os.environ["STORAGE_ACCOUNT_NAME"]
    account_key  = os.environ["STORAGE_ACCOUNT_KEY"]
    conn_str = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={account_name};"
        f"AccountKey={account_key};"
        f"EndpointSuffix=core.windows.net"
    )
    service   = BlobServiceClient.from_connection_string(conn_str)
    container = service.get_container_client(METADATA_CONTAINER_NAME)

    batch_info_raw = (
        container.get_blob_client("batch_info.json")
        .download_blob().readall().decode()
    )
    batch_start_str = json.loads(batch_info_raw)["batch_start"]
    batch_start     = datetime.fromisoformat(batch_start_str)
    if batch_start.tzinfo is None:
        batch_start = batch_start.replace(tzinfo=timezone.utc)

    schemas = []
    for blob in container.list_blobs(name_starts_with="schema_"):
        try:
            ts_str  = blob.name.rsplit("_", 1)[-1].replace(".json", "")
            file_ts = datetime.strptime(ts_str, "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)
        except Exception:
            continue
        if file_ts >= batch_start:
            data = container.get_blob_client(blob.name).download_blob().readall()
            schemas.append(json.loads(data))

    return schemas


# ====================================================================
# DDL UTILITIES  (unchanged from original)
# ====================================================================
def prepare_schemas_for_relationship_detection(schema_files, ddl_result=None):
    schemas = []
    for schema_file in schema_files:
        with open(schema_file, "r") as f:
            schema_data = json.load(f)

        table_name   = schema_data.get("table_name", "")
        columns_data = schema_data.get("columns", [])

        if ddl_result and "ddl_scripts" in ddl_result:
            ddl_script = ddl_result["ddl_scripts"].get(table_name, "")
            columns    = extract_columns_from_ddl(ddl_script) if ddl_script else [
                {"name": c.get("column_name", ""), "type": c.get("data_type", "")}
                for c in columns_data
            ]
        else:
            columns = [
                {"name": c.get("column_name", ""), "type": c.get("data_type", "")}
                for c in columns_data
            ]

        schemas.append({"table_name": table_name, "columns": columns})
    return schemas


def extract_columns_from_ddl(ddl_script):
    """Extract column information from DDL CREATE TABLE statement."""
    columns = []
    match   = re.search(r"\((.*)\)", ddl_script, re.DOTALL)
    if not match:
        return columns

    columns_text       = match.group(1)
    column_definitions = []
    current_def        = ""
    bracket_depth      = 0

    for char in columns_text:
        if char in "<[":
            bracket_depth += 1
        elif char in ">]":
            bracket_depth -= 1
        elif char == "," and bracket_depth == 0:
            column_definitions.append(current_def.strip())
            current_def = ""
            continue
        current_def += char

    if current_def.strip():
        column_definitions.append(current_def.strip())

    for col_def in column_definitions:
        parts = col_def.split()
        if len(parts) >= 2:
            col_name   = parts[0]
            type_parts = []
            for part in parts[1:]:
                if part.upper() in ["NOT", "PRIMARY", "UNIQUE", "NULL"]:
                    break
                type_parts.append(part)
            col_type = " ".join(type_parts)
            if col_type.startswith("ARRAY") or col_type.startswith("STRUCT"):
                col_type = col_type.split("<")[0]
            columns.append({"name": col_name, "type": col_type})

    return columns


def run_relationship_detection(metadata_folder, ddl_analysis_file):
    """Run relationship detection using metadata and DDL analysis."""
    with open(ddl_analysis_file, "r") as f:
        ddl_result = json.load(f)

    schema_files = [
        os.path.join(metadata_folder, f)
        for f in os.listdir(metadata_folder)
        if f.startswith("schema_") and f.endswith(".json")
    ]
    schemas = prepare_schemas_for_relationship_detection(schema_files, ddl_result)

    logging.info("=" * 60)
    logging.info("📋 Prepared Schemas for Relationship Detection:")
    for schema in schemas:
        logging.info(f"\nTable: {schema['table_name']}")
        for col in schema["columns"]:
            logging.info(f"  - {col['name']}: {col['type']}")
    logging.info("=" * 60)

    return detect_relationships(schemas)


# ====================================================================
# AI-BASED SCHEMA → DDL GENERATION  (unchanged from original)
# ====================================================================
def analyze_schemas_with_ai(schemas):
    """Sends schemas to Azure OpenAI and returns DDL JSON."""
    client = AzureOpenAI(
        api_key=AI_API_KEY,
        api_version="2024-05-01-preview",
        azure_endpoint=AI_ENDPOINT
    )

    prompt = (
        "You are a strict JSON generator.\n"
        "Return ONLY valid JSON. No explanations, no markdown.\n\n"
        "Output format:\n"
        "{\n"
        '  "ddl_scripts": {\n'
        '     "<table>": "CREATE TABLE dbo.<table>(...);"  \n'
        "  }\n"
        "}\n\n"
        "Rules:\n"
        "- Fabric-compatible datatypes only\n"
        "- Exactly ONE PRIMARY KEY per table\n"
        "- No GO inside JSON\n"
        "- No markdown\n\n"
        "SCHEMAS:\n"
        f"{json.dumps(schemas, indent=2, cls=NumpyEncoder)}"
    )

    try:
        max_retries = 5
        for attempt in range(max_retries):
            try:
                response = client.chat.completions.create(
                    model=AI_MODEL,
                    messages=[
                        {"role": "system", "content": "Return ONLY valid JSON. No markdown. No explanations. No trailing commas."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0,
                    max_tokens=_estimate_max_tokens(schemas)
                )
                content = response.choices[0].message.content.strip()
                return _extract_json_from_text(content)
            except Exception as e:
                if "429" in str(e) or "rate_limit" in str(e).lower():
                    if attempt < max_retries - 1:
                        wait_time = (2 ** attempt) * 5
                        logging.warning(f"⚠️ Rate limited. Waiting {wait_time}s")
                        time.sleep(wait_time)
                    else:
                        raise
                else:
                    raise
    except Exception:
        logging.exception("AI DDL generation failed. Using fallback.")
        return _local_ddl_fallback(schemas)


def _local_ddl_fallback(schemas):
    """Generate DDL scripts locally without AI."""
    ddl = {"ddl_scripts": {}}

    if isinstance(schemas, dict) and "schemas" in schemas:
        schemas = schemas["schemas"]

    for schema in schemas:
        tname        = schema["table_name"]
        cols         = []
        pk           = None
        pk_candidates = []

        for col in schema["columns"]:
            cname    = col["column_name"]
            dtype    = col["data_type"].lower()
            nullable = col["nullable"]

            if "int"  in dtype:                        sql_type = "INT"
            elif "float" in dtype:                     sql_type = "FLOAT"
            elif "date" in dtype or "time" in dtype:   sql_type = "DATETIME2"
            elif "bool" in dtype:                      sql_type = "BIT"
            else:                                      sql_type = "NVARCHAR(255)"

            cols.append(f"    [{cname}] {sql_type} {'NULL' if nullable else 'NOT NULL'}")

            if col["is_potential_key"]:
                pk_candidates.append(cname)

        if pk_candidates:
            pk = pk_candidates[0]

        sql = f"CREATE TABLE dbo.{tname} (\n"
        sql += ",\n".join(cols)
        if pk:
            sql += f",\n    PRIMARY KEY([{pk}])"
        sql += "\n);"

        ddl["ddl_scripts"][tname] = sql

    return ddl


def generate_fabric_compatible_ddl(analysis):
    """Generate final DDL script with GO statements."""
    ddl_parts = []
    for tname, script in analysis.get("ddl_scripts", {}).items():
        sanitized = _sanitize_sql_script(script)
        ddl_parts.append(sanitized + "\nGO\n")
    return "".join(ddl_parts)


def _sanitize_sql_script(script: str) -> str:
    """Remove markdown and formatting from SQL scripts."""
    if not script:
        return ""
    s = script.strip()
    s = re.sub(r"```sql", "", s, flags=re.IGNORECASE)
    s = re.sub(r"```", "", s)
    s = re.sub(r"\bGO\b", "", s, flags=re.IGNORECASE)
    s = s.replace("`", "")
    if not s.endswith(";"):
        s += ";"
    return s
