# =====================================================
# FILE: services/ner/ner_service.py
# =====================================================

from io import BytesIO
import json
import pandas as pd
import time

from azure.storage.blob import BlobServiceClient

from config import (
    USER_CONTAINER,
    V_CONNECTION_STRING,
    V_DATASET_CONTAINER,
    AI_MODEL
)


class NERService:

    def __init__(self, client):

        self.client = client

        self.blob_service = BlobServiceClient.from_connection_string(
            V_CONNECTION_STRING
        )


    # =====================================================
    # LOAD DATASET
    # =====================================================

    def load_dataset(self, blob_path):

        container = V_DATASET_CONTAINER

        blob = self.blob_service.get_blob_client(
            container=container,
            blob=blob_path
        )

        if not blob.exists():

            raise Exception(
                f"Dataset not found: {blob_path}"
            )

        df = pd.read_csv(
            BytesIO(blob.download_blob().readall())
        )

        return df, blob

    # =====================================================
    # SAVE DATASET
    # =====================================================

    def save_dataset(self, df, blob):

        output = BytesIO()

        df.to_csv(output, index=False)

        output.seek(0)

        blob.upload_blob(
            output.getvalue(),
            overwrite=True
        )

    # =====================================================
    # DETECT ENTITIES IN BATCH
    # =====================================================

    def detect_entities_batch(self, values):

        prompt = f"""
Identify entity types for these values.

Values:
{values}

Allowed entity types:
- PERSON
- LOCATION
- ORGANIZATION
- PRODUCT
- EMAIL
- PHONE
- UNKNOWN

Return ONLY valid JSON.

Format:
{{
  "value": "ENTITY_TYPE"
}}

Example:
{{
  "Hyderabad": "LOCATION",
  "John": "PERSON",
  "iPhone": "PRODUCT",
  "john@gmail.com": "EMAIL"
}}
"""

        response = self.client.chat.completions.create(
            model=AI_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": """
Return ONLY valid JSON.
Do not include markdown.
"""
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=0
        )

        raw = response.choices[0].message.content.strip()

        # =================================================
        # CLEAN MARKDOWN
        # =================================================

        raw = raw.replace("```json", "")
        raw = raw.replace("```", "")
        raw = raw.strip()

        try:
            return json.loads(raw)
        except Exception as e:
            print(f"NER JSON Error: {e}")
            return {}
    
    def resolve_entities_batch(self, values):

        prompt = f"""
    You are an entity resolution engine.

    Analyze these values.

    Identify values that represent the same real-world entity
    and provide a standardized value.

    Only resolve values that are clearly abbreviations,
aliases,
alternate spellings,
or duplicate representations of the same entity.

Do NOT rename:
- business terms
- financial metrics
- product names
- valid values

Examples:

NYC -> New York
LA -> Los Angeles

Do NOT do:

Revenue -> Total Revenue
Profit -> Net Profit
EBITDA -> Earnings

    Return ONLY valid JSON.

    Format:

    {{
    "original_value": {{
        "resolved_value": "standardized value",
        "confidence": 95
    }}
    }}

    Do NOT convert official city names
to alternative official city names.

Examples:

Do NOT change:
Bangalore -> Bengaluru
Mumbai -> Bombay
Kolkata -> Calcutta
Chennai -> Madras

Only resolve:
NYC -> New York
LA -> Los Angeles
Hyd -> Hyderabad

   Only include values that are abbreviations,
aliases,
misspellings,
or duplicate representations
of the same entity.

Examples:

NYC -> New York
LA -> Los Angeles
Hyd -> Hyderabad

Do NOT change:

Bangalore -> Bengaluru
Mumbai -> Bombay
Revenue -> Total Revenue
Profit -> Net Profit
Aarav -> Aarav

Only return values when the original
value is clearly an abbreviation,
alias, or incorrect representation.

    Examples:

    Input:
    ["NYC", "New York", "LA", "Los Angeles"]

    Output:
    {{
    "NYC": {{
        "resolved_value": "New York",
        "confidence": 100
    }},
    "LA": {{
        "resolved_value": "Los Angeles",
        "confidence": 95
    }}
    }}

    IMPORTANT:

Only return values that actually need standardization.

Do NOT return values that are already correct.

Examples:

Return:
NYC -> New York
LA -> Los Angeles
Hyd -> Hyderabad

Do NOT return:
Aarav -> Aarav
Mumbai -> Mumbai
Patel -> Patel
Revenue -> Revenue

    Values:
    {values}
    """

        response = self.client.chat.completions.create(
            model=AI_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": """
    Return ONLY valid JSON.
    Do not include markdown.
    """
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=0
        )

        raw = response.choices[0].message.content.strip()

        raw = raw.replace("```json", "")
        raw = raw.replace("```", "")
        raw = raw.strip()

        try:
            return json.loads(raw)
        except Exception as e:
            print(f"Resolution JSON Error: {e}")
            return {}

    # =====================================================
    # GET VALID NER COLUMNS
    # =====================================================

    def get_ner_columns(self, df):

        ner_cols = []

        skip_keywords = [
            "status",
            "payment",
            "category"
        ]

        for col in df.columns:

            col_lower = col.lower()

            # ==============================================
            # SKIP ENTITY COLUMNS
            # ==============================================

            if col_lower.endswith("_entity"):
                continue

            # ==============================================
            # SKIP NUMERIC COLUMNS
            # ==============================================

            if pd.api.types.is_numeric_dtype(df[col]):
                continue

            # ==============================================
            # SKIP IDS
            # ==============================================

            if "id" in col_lower:
                continue

            # ==============================================
            # SKIP OPERATIONAL COLUMNS
            # ==============================================

            if any(
                keyword in col_lower
                for keyword in skip_keywords
            ):
                continue

            # ==============================================
            # SKIP DATE COLUMNS
            # ==============================================

            try:

                parsed_dates = pd.to_datetime(
                    df[col],
                    errors="coerce",
                    format="mixed"
                )

                date_ratio = parsed_dates.notna().mean()

                # if >80% parsable as dates
                if date_ratio > 0.8:
                    continue

            except:
                pass

            # ==============================================
            # ADD VALID COLUMN
            # ==============================================

            ner_cols.append(col)

        return ner_cols

    # =====================================================
    # RUN NER
    # =====================================================

    def run(self, blob_path):

        df, blob = self.load_dataset(blob_path)

        # ==============================================
        # GET VALID TEXT COLUMNS
        # ==============================================

        text_cols = self.get_ner_columns(df)

        entity_summary = {}

        resolutions = []

        rows_processed = len(df)

        # =================================================
        # PROCESS EACH COLUMN
        # =================================================

        for col in text_cols:

            try:

                print(f"\n🔍 Running NER for column: {col}")

                # ==========================================
                # UNIQUE VALUES ONLY
                # ==========================================

                unique_values = (
                    df[col]
                    .dropna()
                    .astype(str)
                    .str.strip()
                    .unique()
                    .tolist()
                )

                # ==========================================
                # LIMIT VALUES
                # ==========================================

                unique_values = unique_values[:50]

                if not unique_values:
                    continue

                # ==========================================
                # ENTITY DETECTION
                # ==========================================

                start = time.time()

                entity_map = self.detect_entities_batch(
                    unique_values
                )

                print(
                    f"NER {col} took {time.time() - start:.2f}s"
                )

                detected_types = set(entity_map.values())

                # ==========================================
                # ENTITY COUNTS (NO NEW COLUMNS)
                # ==========================================

                value_counts = (
                    df[col]
                    .dropna()
                    .astype(str)
                    .str.strip()
                    .value_counts()
                    .to_dict()
                )

                for value, count in value_counts.items():

                    entity = entity_map.get(
                        value,
                        "UNKNOWN"
                    )

                    entity_summary[entity] = (
                        entity_summary.get(entity, 0)
                        + int(count)
                    )

                # ==========================================
                # ENTITY RESOLUTION
                # ==========================================

                resolution_map = {}

                if (
                    "LOCATION" in detected_types
                    or "ORGANIZATION" in detected_types
                ):

                    start = time.time()

                    resolution_map = self.resolve_entities_batch(
                        unique_values
                    )

                    print(
                        f"Resolution {col} took {time.time() - start:.2f}s"
                    )

                # ==========================================
                # STORE RESOLUTION REPORT
                # ==========================================

                valid_resolution_map = {}

                for original, info in resolution_map.items():

                    resolved_value = info.get(
                        "resolved_value",
                        original
                    )

                    confidence = info.get(
                        "confidence",
                        100
                    )

                    # Skip if nothing changed
                    if (
                        str(original).strip().lower()
                        ==
                        str(resolved_value).strip().lower()
                    ):
                        continue

                    # Skip low confidence
                    if confidence < 95:
                        continue

                    valid_resolution_map[original] = resolved_value

                    resolutions.append(
                        {
                            "column": col,
                            "original": original,
                            "resolved": resolved_value,
                            "confidence": confidence
                        }
                    )

                # ==========================================
                # OVERWRITE ORIGINAL VALUES
                # ==========================================

                df[col] = df[col].apply(
                    lambda x:
                    valid_resolution_map.get(
                        str(x).strip(),
                        x
                    )
                    if pd.notna(x)
                    else x
                )

            except Exception as e:

                print(
                    f"⚠️ NER skipped for {col}: {str(e)}"
                )

                continue

        # =================================================
        # SAVE UPDATED DATASET
        # =================================================

        self.save_dataset(df, blob)

        summary = []

        if entity_summary.get("PERSON"):
            summary.append(
                f"Identified {entity_summary['PERSON']} customer/person records"
            )

        if entity_summary.get("EMAIL"):
            summary.append(
                f"Identified {entity_summary['EMAIL']} email addresses"
            )

        if entity_summary.get("LOCATION"):
            summary.append(
                f"Identified {entity_summary['LOCATION']} location records"
            )

        if entity_summary.get("PRODUCT"):
            summary.append(
                f"Identified {entity_summary['PRODUCT']} product records"
            )

        if len(resolutions) > 0:
            summary.append(
                f"Standardized {len(resolutions)} values across the dataset"
            )
        else:
            summary.append(
                "No value standardization was required"
            )

        # =================================================
        # RESPONSE
        # =================================================

        return {
            "status": "success",
            "blob_path": blob_path,
            "rows_processed": rows_processed,
            "columns_processed": len(text_cols),
            "summary": summary,

            # reporting only
            "entities_detected": entity_summary,

            # actual resolutions applied
            "resolutions_found": len(resolutions),
            "resolutions": resolutions,

            "dataset_updated": True
        }
