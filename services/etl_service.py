import pandas as pd
import re

from io import BytesIO

from azure.storage.blob import (
    BlobServiceClient
)

from config import (

    BLOB_CONN_STR,

    DATA_INGESTION_CONTAINER
)


class ETLService:

    def __init__(self):

        self.blob = (
            BlobServiceClient
            .from_connection_string(
                BLOB_CONN_STR
            )
        )

    # =====================================================
    # LOGGER
    # =====================================================

    def log(self, msg):

        print(f"[ETL] {msg}")

    # =====================================================
    # NORMALIZE COLUMN
    # =====================================================

    def normalize(self, col):

        return re.sub(

            r'\\W+',

            '_',

            str(col).lower()
        )

    # =====================================================
    # FIND COLUMN
    # =====================================================

    def find_column(

        self,

        df,

        target_col
    ):

        target = self.normalize(
            target_col
        )

        for col in df.columns:

            if (
                self.normalize(col)
                == target
            ):

                return col

        return None

    # =====================================================
    # LOAD CSV
    # =====================================================

    def load(

        self,

        user_id,

        job_id,

        table
    ):

        path = (
            f"{user_id}/"
            f"{job_id}/"
            f"{table}.csv"
        )

        blob = self.blob.get_blob_client(

            container=(
                DATA_INGESTION_CONTAINER
            ),

            blob=path
        )

        data = (
            blob
            .download_blob()
            .readall()
        )

        df = pd.read_csv(
            BytesIO(data)
        )

        # ==========================================
        # NORMALIZE COLUMNS
        # ==========================================

        df.columns = [

            self.normalize(c)

            for c in df.columns
        ]

        return df

    # =====================================================
    # PREFIX COLUMNS
    # =====================================================

    def prefix_columns(

        self,

        df,

        table_name,

        join_key
    ):

        df = df.copy()

        rename_map = {}

        prefix = table_name.rstrip("s")

        for col in df.columns:

            if col == join_key:
                continue

            if col.endswith("_id"):
                continue

            if col.startswith(prefix + "_"):
                continue

            rename_map[col] = (
                f"{prefix}_{col}"
            )

        df.rename(

            columns=rename_map,

            inplace=True
        )

        return df

    # =====================================================
    # SAFE MERGE
    # =====================================================

    def safe_merge(

        self,

        left,

        right,

        lkey,

        rkey,

        table_name
    ):

        if lkey is None or rkey is None:

            self.log(

                f"⚠️ Skipping join "
                f"(missing key): "
                f"{lkey}, {rkey}"
            )

            return left

        if (

            lkey not in left.columns

            or

            rkey not in right.columns
        ):

            self.log(

                f"⚠️ Key not found: "
                f"{lkey}, {rkey}"
            )

            return left

        right = self.prefix_columns(

            right,

            table_name,

            rkey
        )

        before = len(left)

        merged = left.merge(

            right,

            left_on=lkey,

            right_on=rkey,

            how="left"
        )

        after = len(merged)

        # ==========================================
        # PREVENT EXPLOSION
        # ==========================================

        if after > before * 2:

            self.log(

                "⚠️ Merge explosion detected"
            )

            return left

        return merged

    # =====================================================
    # EXECUTE ETL
    # =====================================================

    def execute(

        self,

        dataset,

        user_id,

        job_id,

        model_output
    ):

        model = (
            model_output["model_output"]["data"]
        )

        tables = {}

        relationships = (
            model["relationships"]
        )

        fact = (
            model["model"]["fact_table"]
        )

        # ==========================================
        # LOAD TABLES
        # ==========================================

        for t in model["tables"]:

            name = t["table_name"]

            try:

                tables[name] = self.load(

                    user_id,

                    job_id,

                    name
                )

            except Exception as e:

                self.log(

                    f"⚠️ Failed loading "
                    f"{name}: {e}"
                )

                continue

        self.log(
            f"Tables: {list(tables.keys())}"
        )

        self.log(
            f"Fact: {fact}"
        )

        self.log(
            f"Relationships: {relationships}"
        )

        if fact not in tables:

            raise Exception(
                f"Fact table "
                f"{fact} not found"
            )

        # ==========================================
        # START WITH FACT TABLE
        # ==========================================

        df = tables[fact]

        joined = set()

        # ==========================================
        # FACT → DIM
        # ==========================================

        for rel in relationships:

            if rel["from_table"] != fact:
                continue

            right = rel["to_table"]

            if right not in tables:
                continue

            lkey = self.find_column(

                df,

                rel["from_column"]
            )

            rkey = self.find_column(

                tables[right],

                rel["to_column"]
            )

            self.log(

                f"Joining "
                f"{fact} → {right} "
                f"on {lkey}={rkey}"
            )

            df = self.safe_merge(

                df,

                tables[right],

                lkey,

                rkey,

                right
            )

            joined.add(right)

        # ==========================================
        # CONTROLLED DIMENSION JOIN
        # ==========================================

        queue = list(joined)

        while queue:

            current = queue.pop(0)

            for rel in relationships:

                if (
                    rel["from_table"]
                    != current
                ):

                    continue

                right = rel["to_table"]

                if right in joined:
                    continue

                if right not in tables:
                    continue

                lkey = self.find_column(

                    df,

                    rel["from_column"]
                )

                rkey = self.find_column(

                    tables[right],

                    rel["to_column"]
                )

                self.log(

                    f"Joining "
                    f"{current} → {right} "
                    f"on {lkey}={rkey}"
                )

                df = self.safe_merge(

                    df,

                    tables[right],

                    lkey,

                    rkey,

                    right
                )

                joined.add(right)

                queue.append(right)

        # ==========================================
        # FINAL OUTPUT
        # ==========================================

        self.log(
            f"Final rows: {len(df)}"
        )

        self.log(
            f"Final columns: {len(df.columns)}"
        )

        # ==========================================
        # CSV BUFFER
        # ==========================================

        buffer = BytesIO()

        df.to_csv(

            buffer,

            index=False
        )

        buffer.seek(0)

        # ==========================================
        # FINAL RESPONSE
        # ==========================================

        return {

            "file_buffer": buffer,

            "rows": len(df),

            "columns": list(df.columns),

            # IMPORTANT FIX
            "dataframe": df,

            "preview": (

                df.head(10)
                .to_dict(
                    orient="records"
                )
            )
        }
