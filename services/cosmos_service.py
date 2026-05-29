import datetime

from azure.cosmos import (

    CosmosClient,

    PartitionKey
)

from config import (

    COSMOS_URL,

    COSMOS_KEY
)

DATABASE_NAME = "AuthDB"

CONTAINER_NAME = "Users"


class CosmosService:

    def __init__(self):

        self.client = CosmosClient(

            COSMOS_URL,

            credential=COSMOS_KEY
        )

        self.db = (

            self.client
            .create_database_if_not_exists(
                DATABASE_NAME
            )
        )

        self.container = (

            self.db
            .create_container_if_not_exists(

                id=CONTAINER_NAME,

                partition_key=PartitionKey(
                    path="/id"
                )
            )
        )
    
    

    # =====================================================
    # GET USER DOCUMENT
    # =====================================================


    def get_user_document(self, user_id):

        try:

            user_doc = self.container.read_item(
                item=user_id,
                partition_key=user_id
            )

            return user_doc

        except Exception as e:

            print("❌ Cosmos query failed")

            print(str(e))

            return None

    # =====================================================
    # SAVE / UPDATE JOB
    # =====================================================

    def save_dataset(

        self,

        user_id,

        job_doc
    ):

        print(
            f"💾 Saving dataset "
            f"for user: {user_id}"
        )

        # ==========================================
        # LOAD USER DOC
        # ==========================================

        user_doc = self.get_user_document(
            user_id
        )

        if not user_doc:

            raise Exception(
                f"User not found: {user_id}"
            )

        # ==========================================
        # CREATE JOBS ARRAY
        # ==========================================

        if "jobs" not in user_doc:

            user_doc["jobs"] = []

        # ==========================================
        # REMOVE EXISTING JOB
        # ==========================================

        user_doc["jobs"] = [

            j for j in user_doc["jobs"]

            if (
                j.get("job_id")
                !=
                job_doc.get("job_id")
            )
        ]

        # ==========================================
        # APPEND NEW JOB
        # ==========================================

        user_doc["jobs"].append(
            job_doc
        )

        print(
            f"✅ Jobs count: "
            f"{len(user_doc['jobs'])}"
        )

        # ==========================================
        # SAVE BACK
        # ==========================================

        self.container.upsert_item(
            user_doc
        )

        print(
            "✅ Cosmos save success"
        )

        return job_doc

    # =====================================================
    # GET JOB
    # =====================================================

    def get_dataset(

        self,

        user_id,

        job_id
    ):

        user_doc = self.get_user_document(
            user_id
        )

        if not user_doc:

            return None

        jobs = user_doc.get(
            "jobs",
            []
        )

        for job in jobs:

            if (
                job.get("job_id")
                ==
                job_id
            ):

                return job

        return None

    # =====================================================
    # UPDATE JOB
    # =====================================================

    def update_dataset(

        self,

        user_id,

        job_doc
    ):

        return self.save_dataset(

            user_id,

            job_doc
        )

    # =====================================================
    # LIST JOBS
    # =====================================================

    def list_jobs(
        self,
        user_id
    ):

        user_doc = self.get_user_document(
            user_id
        )

        if not user_doc:

            return []

        jobs = user_doc.get(
            "jobs",
            []
        )

        jobs.sort(

            key=lambda x: x.get(
                "created_at",
                ""
            ),

            reverse=True
        )

        return jobs

    # =====================================================
    # DELETE JOB
    # =====================================================

    def delete_job(
        self,

        user_id,

        job_id
    ):

        user_doc = self.get_user_document(
            user_id
        )

        if not user_doc:

            return False

        jobs = user_doc.get(
            "jobs",
            []
        )

        user_doc["jobs"] = [

            j for j in jobs

            if (
                j.get("job_id")
                !=
                job_id
            )
        ]

        self.container.upsert_item(
            user_doc
        )
        print(
            "✅ Cosmos save success"
        )


        return True
    # =====================================================
    # SAVE CREATED DATASET
    # =====================================================

    def save_create_dataset(

        self,

        user_id,

        dataset_doc
    ):

        print(
            f"💾 Saving created dataset "
            f"for user: {user_id}"
        )

        # ==========================================
        # LOAD USER DOC
        # ==========================================

        user_doc = self.get_user_document(
            user_id
        )

        if not user_doc:

            raise Exception(
                f"User not found: {user_id}"
            )

        # ==========================================
        # CREATE ARRAY
        # ==========================================

        if "create_datasets" not in user_doc:

            user_doc["create_datasets"] = []

        # ==========================================
        # REMOVE EXISTING
        # ==========================================

        user_doc["create_datasets"] = [

            d for d in user_doc[
                "create_datasets"
            ]

            if (
                d.get("job_id")
                !=
                dataset_doc.get("job_id")
            )
        ]

        # ==========================================
        # APPEND
        # ==========================================

        user_doc["create_datasets"].append(
            dataset_doc
        )

        # ==========================================
        # SAVE
        # ==========================================

        self.container.upsert_item(
            user_doc
        )

        print(
            "✅ Created dataset saved"
        )

        return dataset_doc
 
    # =====================================================
    # RENAME CREATED DATASET
    # =====================================================

    def rename_created_dataset(

        self,

        user_id,

        job_id,

        new_dataset_name,

        new_dataset_path,

        new_onelake_path
    ):

        user_doc = self.get_user_document(
            user_id
        )

        if not user_doc:

            return

        created_datasets = user_doc.get(
            "created_datasets",
            []
        )

        for dataset in created_datasets:

            if dataset.get("job_id") == job_id:

                dataset["custom_table_name"] = (
                    new_dataset_name
                )

                dataset["file_path"] = (
                    new_dataset_path
                )

                dataset["onelake_path"] = (
                    new_onelake_path
                )

        self.container.upsert_item(
            user_doc
        )

        print(
            "✅ Created dataset renamed"
        )


    # =====================================================
    # RENAME JOB
    # =====================================================

    def rename_job(

        self,

        user_id,

        job_id,

        new_job_name
    ):

        user_doc = self.get_user_document(
            user_id
        )

        if not user_doc:

            return None

        jobs = user_doc.get(
            "jobs",
            []
        )

        updated_job = None

        for job in jobs:

            if job.get("job_id") == job_id:

                job["job_name"] = (
                    new_job_name
                )

                updated_job = job

                break

        self.container.upsert_item(
            user_doc
        )

        print(
            "✅ Job renamed in Cosmos"
        )

        return updated_job

    # =====================================================
    # UPDATE PIPELINE
    # =====================================================

    def update_pipeline(

        self,

        user_id,

        job_id,

        pipeline
    ):

        user_doc = self.get_user_document(
            user_id
        )

        if not user_doc:

            return None

        jobs = user_doc.get(
            "jobs",
            []
        )

        updated_job = None

        for job in jobs:

            if job.get("job_id") == job_id:

                job["pipeline"] = pipeline

                job["scheduled"] = True

                job["updated_at"] = (

                    datetime.utcnow()
                    .isoformat()

                    + "Z"
                )

                updated_job = job

                break

        self.container.upsert_item(
            user_doc
        )

        print(
            "✅ Pipeline updated in Cosmos"
        )

        return updated_job



