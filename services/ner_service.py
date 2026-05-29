# =====================================================
# FILE: services/ner/ner_service.py
# =====================================================

from io import BytesIO
import json
import pandas as pd

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

        return json.loads(raw)

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

        entity_columns = []

        entity_summary = {}

        rows_processed = len(df)

        # =================================================
        # PROCESS EACH COLUMN
        # =================================================

        for col in text_cols:

            try:

                print(f"\n🔍 Running NER for column: {col}")

                entity_col = f"{col}_entity"

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
                # DETECT ENTITIES
                # ==========================================

                entity_map = self.detect_entities_batch(
                    unique_values
                )

                # ==========================================
                # MAP BACK TO DATAFRAME
                # ==========================================

                df[entity_col] = (
                    df[col]
                    .astype(str)
                    .str.strip()
                    .map(entity_map)
                    .fillna("UNKNOWN")
                )

                entity_columns.append(entity_col)

                # ==========================================
                # SUMMARY COUNTS
                # ==========================================

                counts = (
                    df[entity_col]
                    .value_counts()
                    .to_dict()
                )

                for entity, count in counts.items():

                    entity_summary[entity] = (
                        entity_summary.get(entity, 0)
                        + int(count)
                    )

            except Exception as e:

                print(
                    f"⚠️ NER skipped for {col}: {str(e)}"
                )

                continue

        # =================================================
        # SAVE ENRICHED DATASET
        # =================================================

        self.save_dataset(df, blob)

        # =================================================
        # RESPONSE
        # =================================================

        return {
            "status": "success",
            "blob_path": blob_path,
            "rows_processed": rows_processed,
            "columns_processed": len(text_cols),
            "entity_columns_created": entity_columns,
            "entities_detected": entity_summary
        }