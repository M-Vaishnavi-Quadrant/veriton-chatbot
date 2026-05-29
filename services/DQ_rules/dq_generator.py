# =====================================================
# FILE: services/DQ_rules/dq_generator.py
# =====================================================

import json
import pandas as pd
import numpy as np

from io import BytesIO

from azure.storage.blob import BlobServiceClient

from config import (
    V_CONNECTION_STRING,
    V_DATASET_CONTAINER,
    AZURE_OPENAI_DEPLOYMENT
)


class DQRuleGenerator:

    def __init__(self, client):

        self.client = client

        self.blob_service = BlobServiceClient.from_connection_string(
            V_CONNECTION_STRING
        )

    # =====================================================
    # LOAD DATASET
    # =====================================================

    def load_dataset(self, blob_path):

        blob_client = self.blob_service.get_blob_client(
            container=V_DATASET_CONTAINER,
            blob=blob_path
        )

        if not blob_client.exists():

            raise Exception(
                f"Dataset not found: {blob_path}"
            )

        data = blob_client.download_blob().readall()

        df = pd.read_csv(BytesIO(data))

        return df

    # =====================================================
    # GENERATE RULES
    # =====================================================

    def generate_rules(self, blob_path):

        # ==============================================
        # LOAD DATASET
        # ==============================================

        df = self.load_dataset(blob_path)

        sample = (
            df.head(20)
            .replace({np.nan: None})
            .to_dict(orient="records")
        )

        columns = df.columns.tolist()

        # ==============================================
        # PROMPT
        # ==============================================

        prompt = f"""
You are an enterprise Data Quality Rule Engine.

Generate business-friendly DQ validation rules.

Dataset Columns:
{columns}

Dataset Sample:
{sample}

SUPPORTED RULE TYPES:

1. Not Null
Example:
"customer_id must not be null"

2. Unique
Example:
"transaction_id must be unique"

3. Email Validation
Example:
"customer_email must be valid email"

4. Numeric Range
Example:
"quantity must be greater than 0"

5. Date Validation
Example:
"order_date must be valid date"

IMPORTANT:
Only generate rules using EXISTING columns.

Return ONLY valid JSON.

Format:
[
  {{
    "rule": "",
    "description": "",
    "severity": "high|medium|low"
  }}
]
"""

        # ==============================================
        # LLM
        # ==============================================

        response = self.client.chat.completions.create(

            model=AZURE_OPENAI_DEPLOYMENT,

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

        raw = response.choices[
            0
        ].message.content

        # ==============================================
        # PARSE
        # ==============================================

        rules = json.loads(raw)

        return {

            "success": True,

            "data": {

                "blob_path": blob_path,

                "rules_count": len(rules),

                "file": rules
            }
        }