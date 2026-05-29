import uuid

from agent.prompt_parser import (
    parse_prompt
)

from services.ingestion_service import (
    IngestionService
)

from services.datamodel_service import (
    DataModelService
)

from services.dataset_service import (
    DatasetService
)

from services.etl_service import (
    ETLService
)


class PipelineService:

    def __init__(self):

        self.ingestion = (
            IngestionService()
        )

        self.datamodel = (
            DataModelService()
        )

        self.dataset = (
            DatasetService()
        )

        self.etl = (
            ETLService()
        )

    # =====================================================
    # RUN PIPELINE
    # =====================================================

    def run(

        self,

        prompt,

        user_id,

        job_id=None
    ):

        if not job_id:

            job_id = str(uuid.uuid4())

        print(
            "\n🚀 PIPELINE STARTED"
        )

        # ==========================================
        # STEP 1: PARSE PROMPT
        # ==========================================

        plan = parse_prompt(prompt)

        sources = plan.get(
            "sources",
            []
        )

        if not sources:

            raise Exception(
                "No sources detected"
            )

        # ==========================================
        # STEP 2: INGESTION
        # ==========================================

        self.ingestion.ingest_sources(

            sources=sources,

            user_id=user_id,

            job_id=job_id
        )

        # ==========================================
        # STEP 3: DATA MODEL
        # ==========================================

        model_output = (
            self.datamodel.execute(

                user_id=user_id,

                job_id=job_id
            )
        )

        model_data = (
            model_output["model_output"]["data"]
        )

        fact_table = (

            model_data.get(
                "fact_override"
            )

            or

            model_data["model"].get(
                "fact_table"
            )
        )

        if not fact_table:

            raise Exception(
                "Fact table not found"
            )

        # ==========================================
        # STEP 4: DATASET
        # ==========================================

        dataset_name = (

            f"{fact_table.lower()}"
            f"_dataset.csv"
        )

        dataset_output = (

            self.dataset.execute(

                model_output=model_output,

                user_id=user_id,

                job_id=job_id,

                dataset_name=dataset_name
            )
        )

        # ==========================================
        # STEP 5: ETL
        # ==========================================

        etl_output = self.etl.execute(

            dataset=dataset_output["dataset"],

            user_id=user_id,

            job_id=job_id,

            model_output=model_output
        )

        # ==========================================
        # VALIDATE ETL
        # ==========================================

        file_buffer = (
            etl_output.get("file_buffer")
        )

        if not file_buffer:

            raise Exception(
                "Missing file buffer from ETL"
            )

        # ==========================================
        # GET DATAFRAME
        # ==========================================

        final_df = (
            etl_output.get("dataframe")
        )

        if final_df is None:

            raise Exception(
                "ETL did not return dataframe"
            )

        print(
            f"[ETL] Final rows: "
            f"{len(final_df)}"
        )

        # ==========================================
        # DIMENSION TABLES
        # ==========================================

        dimension_tables = set()

        for rel in model_data.get(
            "relationships",
            []
        ):

            if (
                rel["from_table"]
                == fact_table
            ):

                dimension_tables.add(
                    rel["to_table"]
                )

        for rel in model_data.get(
            "relationships",
            []
        ):

            if (
                rel["from_table"]
                in dimension_tables
            ):

                dimension_tables.add(
                    rel["to_table"]
                )

        dimension_tables = list(
            dimension_tables
        )

        # ==========================================
        # RELATIONSHIPS
        # ==========================================

        relationships = [

            {
                "from": rel["from_table"],

                "to": rel["to_table"],

                "join": (

                    f"{rel['from_column']}"

                    f" = "

                    f"{rel['to_column']}"
                )
            }

            for rel in model_data.get(
                "relationships",
                []
            )
        ]

        # ==========================================
        # FINAL DATASET
        # ==========================================

        final_dataset = {

            "rows": (
                etl_output.get("rows")
            ),

            "columns": (
                etl_output.get("columns")
            ),

            "preview": (
                etl_output.get("preview")
            ),

            "dataset_name": (
                dataset_name
            ),

            # IMPORTANT
            # dataframe ONLY
            "dataframe": final_df
        }

        # ==========================================
        # FINAL RESPONSE
        # ==========================================

        return {

            "status": "success",

            "data_model": {

                "fact_table": (
                    fact_table
                ),

                "dimension_tables": (
                    dimension_tables
                )
            },

            "relationships": (
                relationships
            ),

            "schemas": (

                model_data.get(
                    "schemas",
                    {}
                )
            ),

            "final_dataset": (
                final_dataset
            ),

            "pipeline_metadata": {

                "prompt": prompt,

                "user_id": user_id,

                "job_id": job_id
            }
        }
