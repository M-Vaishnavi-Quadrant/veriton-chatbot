import pandas as pd

from io import BytesIO

from azure.storage.blob import (
    BlobServiceClient
)

from config import (

    V_CONNECTION_STRING,

    V_DATASET_CONTAINER
)

from services.DQ_rules.dq_generator import (
    DQRuleGenerator
)

from services.DQ_rules.dq_runner import (
    run_dq_pipeline
)



class DQService:

    def __init__(self, client):

        self.client = client

        self.blob_service = (
            BlobServiceClient
            .from_connection_string(
                V_CONNECTION_STRING
            )
        )

        self.rule_generator = (
            DQRuleGenerator(client)
        )


    # =====================================================
    # LOAD DATASET
    # =====================================================

    def load_dataset(
            self,
            blob_path
        ):

            # ==========================================
            # GET BLOB CLIENT
            # ==========================================

            blob_client = (
                self.blob_service
                .get_blob_client(

                    container=V_DATASET_CONTAINER,

                    blob=blob_path
                )
            )

            # ==========================================
            # VALIDATE
            # ==========================================

            if not blob_client.exists():

                raise Exception(
                    f"Dataset not found: "
                    f"{blob_path}"
                )

            # ==========================================
            # DOWNLOAD
            # ==========================================

            data = (
                blob_client
                .download_blob()
                .readall()
            )

            # ==========================================
            # LOAD DATAFRAME
            # ==========================================

            df = pd.read_csv(
                BytesIO(data)
            )

            return df

    # =====================================================
    # SAVE DATASET
    # =====================================================

    def save_dataset(

        self,

        df,

        blob_path
    ):

        buffer = BytesIO()

        df.to_csv(

            buffer,

            index=False
        )

        buffer.seek(0)

        blob_client = (

            self.blob_service
            .get_blob_client(

                container=V_DATASET_CONTAINER,

                blob=blob_path
            )
        )

        blob_client.upload_blob(

            buffer.getvalue(),

            overwrite=True
        )

    # =====================================================
    # MAIN DQ FLOW
    # =====================================================

    def run(
        self,
        blob_path
    ):

        # ==========================================
        # LOAD DATASET
        # ==========================================

        df = self.load_dataset(
            blob_path
        )

        # ==========================================
        # GENERATE RULES
        # ==========================================

        rules_response = (

            self.rule_generator
            .generate_rules(blob_path)
        )

        rules = (
            rules_response["data"]["file"]
        )

        # ==========================================
        # RUN DQ PIPELINE
        # ==========================================

        result = run_dq_pipeline(

            self.client,

            df,

            rules
        )

        fixed_df = result["fixed_df"]

        # ==========================================
        # SAVE FIXED DATASET
        # ==========================================

        self.save_dataset(

            fixed_df,

            blob_path
        )

        # ==========================================
        # RESPONSE
        # ==========================================

        return {

            "status": "success",

            "rules_applied": len(rules),

            "rules": rules,

            "issues_before": (
                result["issues_before"]
            ),

            "issues_after": (
                result["issues_after"]
            ),

            "proposed_solutions": (
                result[
                    "proposed_solutions"
                ]
            ),

            "columns_checked": (
                result[
                    "columns_checked"
                ]
            ),

            "dataset_path": blob_path
        }