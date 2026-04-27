# import pandas as pd
# from azure.storage.blob import BlobServiceClient
# import os
# import re

# from app.config import BLOB_CONN_STR, DATA_INGESTION_CONTAINER


# class ETLService:

#     def __init__(self):
#         self.blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)

#     # =====================================================
#     # NORMALIZE COLUMN
#     # =====================================================
#     def normalize_column(self, col):
#         col = str(col).strip()
#         col = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', col)
#         col = col.lower()
#         col = col.replace(" ", "_").replace("__", "_")
#         return col
    
#     def column_similarity(self, c1, c2):
#         c1 = c1.replace("_id", "")
#         c2 = c2.replace("_id", "")

#         if c1 == c2:
#             return 1.0

#         if c1 in c2 or c2 in c1:
#             return 0.7

#         return 0.0

#     # =====================================================
#     # LOAD CSV
#     # =====================================================
#     def load_csv(self, user_id, job_id, file_name):

#         file_name = file_name.lower()
#         blob_path = f"{user_id}/{job_id}/{file_name}.csv"

#         blob_client = self.blob_service.get_blob_client(
#             container=DATA_INGESTION_CONTAINER,
#             blob=blob_path
#         )

#         data = blob_client.download_blob().readall()

#         os.makedirs("tmp", exist_ok=True)
#         local_path = os.path.join("tmp", f"{file_name}.csv")

#         with open(local_path, "wb") as f:
#             f.write(data)

#         df = pd.read_csv(local_path)
#         df.columns = [self.normalize_column(c) for c in df.columns]

#         return df

#     # =====================================================
#     # FIND COLUMN
#     # =====================================================
#     def find_column(self, df, target):

#         target = self.normalize_column(target)

#         if target in df.columns:
#             return target

#         for col in df.columns:
#             if target in col or col in target:
#                 return col

#         return None

#     # =====================================================
#     # DYNAMIC PREFIXING
#     # =====================================================
#     def prefix_columns(self, df, table_name, join_key):

#         df = df.copy()
#         rename_map = {}

#         prefix = table_name.rstrip("s")

#         for col in df.columns:

#             if col == join_key:
#                 continue

#             if col.endswith("_id"):
#                 continue

#             if col.startswith(prefix + "_"):
#                 continue

#             if len(col.split("_")) > 1:
#                 continue

#             rename_map[col] = f"{prefix}_{col}"

#         df.rename(columns=rename_map, inplace=True)
#         return df

#     # =====================================================
#     # SAFE MERGE
#     # =====================================================
#     # def safe_merge(self, left_df, right_df, left_on, right_on, table_name):

#     #     before_rows = len(left_df)

#     #     right_df = self.prefix_columns(right_df, table_name, right_on)

#     #     drop_cols = [
#     #         c for c in right_df.columns
#     #         if c != right_on and c in left_df.columns
#     #     ]

#     #     right_df = right_df.drop(columns=drop_cols, errors="ignore")

#     #     merged = left_df.merge(
#     #         right_df,
#     #         left_on=left_on,
#     #         right_on=right_on,
#     #         how="left"
#     #     )

#     #     after_rows = len(merged)

#     #     # 🚨 SAFETY CHECK: explosion detection
#     #     if after_rows > before_rows * 3:
#     #         return left_df  # reject bad join

#     #     if left_on != right_on and right_on in merged.columns:
#     #         merged.drop(columns=[right_on], inplace=True)

#     #     return merged

#     def safe_merge(self, left_df, right_df, left_on, right_on, table_name):

#         before_rows = len(left_df)

#         merged = left_df.merge(
#             right_df,
#             left_on=left_on,
#             right_on=right_on,
#             how="left"
#         )

#         after_rows = len(merged)

#         # 🚨 Reject bad joins
#         if after_rows > before_rows * 2:
#             return left_df

#         # 🚨 Reject if too many nulls added
#         new_cols = set(merged.columns) - set(left_df.columns)

#         if new_cols:
#             null_ratio = merged[list(new_cols)].isna().mean().mean()
#             if null_ratio > 0.8:
#                 return left_df

#         if left_on != right_on and right_on in merged.columns:
#             merged.drop(columns=[right_on], inplace=True)

#         return merged
#     # =====================================================
#     # MATCH COLUMNS
#     # =====================================================
#     # def match_columns(self, df1, df2):

#     #     matches = []

#     #     for c1 in df1.columns:
#     #         for c2 in df2.columns:

#     #             if c1 == c2:
#     #                 matches.append((c1, c2))

#     #             elif c1.replace("_id", "") == c2.replace("_id", ""):
#     #                 matches.append((c1, c2))

#     #             elif c1 in c2 or c2 in c1:
#     #                 matches.append((c1, c2))

#     #     return matches

#     # =====================================================
#     # AUTO JOIN
#     # =====================================================
#     def infer_join(self, df1, df2):
#         best_score = 0
#         best_pair = (None, None)

#         for c1 in df1.columns:
#             for c2 in df2.columns:

#                 # -------------------------
#                 # STEP 1: normalize names
#                 # -------------------------
#                 base1 = c1.replace("_id", "")
#                 base2 = c2.replace("_id", "")

#                 if base1 != base2:
#                     continue   # 🚨 STRICT MATCH ONLY

#                 try:
#                     s1 = df1[c1].dropna()
#                     s2 = df2[c2].dropna()

#                     if s1.empty or s2.empty:
#                         continue

#                     set1 = set(s1)
#                     set2 = set(s2)

#                     intersection = set1.intersection(set2)

#                     if not intersection:
#                         continue

#                     # -------------------------
#                     # STEP 2: overlap
#                     # -------------------------
#                     overlap = len(intersection) / min(len(set1), len(set2))

#                     # -------------------------
#                     # STEP 3: uniqueness
#                     # -------------------------
#                     uniq1 = len(set1) / len(df1)
#                     uniq2 = len(set2) / len(df2)

#                     # -------------------------
#                     # STEP 4: FK vs PK logic
#                     # -------------------------
#                     fk_score = abs(uniq1 - uniq2)

#                     # We want one high unique, one low unique
#                     if not (uniq1 > 0.9 or uniq2 > 0.9):
#                         continue

#                     # -------------------------
#                     # STEP 5: final score
#                     # -------------------------
#                     score = (
#                         overlap * 0.5 +
#                         fk_score * 0.3 +
#                         max(uniq1, uniq2) * 0.2
#                     )

#                     if score > best_score:
#                         best_score = score
#                         best_pair = (c1, c2)

#                 except:
#                     continue

#         if best_score >= 0.6:
#             return best_pair

#         return None, None
    
#     def remove_semantic_duplicates(self, df):

#         seen = {}
#         drop_cols = []

#         for col in df.columns:
#             base = col.split("_")[-1]

#             if base in seen:
#                 drop_cols.append(col)
#             else:
#                 seen[base] = col

#         return df.drop(columns=drop_cols, errors="ignore")

#     # =====================================================
#     # CLEAN FINAL COLUMNS
#     # =====================================================
#     def clean_columns(self, df):

#         # -----------------------------------------
#         # 1. Remove exact duplicate columns
#         # -----------------------------------------
#         df = df.loc[:, ~df.columns.duplicated()]

#         # -----------------------------------------
#         # 2. Remove merge suffixes (_x, _y)
#         # -----------------------------------------
#         df.columns = [
#             col.replace("_x", "").replace("_y", "")
#             for col in df.columns
#         ]

#         # -----------------------------------------
#         # 3. Remove semantic duplicates (FINAL FIX)
#         # -----------------------------------------
#         df = self.remove_semantic_duplicates(df)

#         return df
    
    

#     # =====================================================
#     # MAIN EXECUTION
#     # =====================================================
#     def execute(self, dataset, user_id, job_id, model_output):

#         tables = {}
#         column_lineage = {}   # 🔥 NEW

#         model_data = model_output["model_output"]["data"]

#         # TABLE MAPPING
#         table_mapping = {
#             t["table_name"]: (t.get("derived_from") or t["table_name"]).lower()
#             for t in model_data["tables"]
#         }

#         # LOAD TABLES
#         for table, file_name in table_mapping.items():

#             try:
#                 df = self.load_csv(user_id, job_id, file_name)

#                 # 🔥 TRACK ORIGINAL COLUMNS
#                 for col in df.columns:
#                     column_lineage[col] = table

#                 tables[table] = df

#             except:
#                 continue

#         if not tables:
#             raise Exception("❌ No tables loaded")

#         # FACT TABLE
#         fact_table = model_data["model"]["fact_table"]

#         if fact_table not in tables:
#             fact_table = max(tables, key=lambda k: len(tables[k]))

#         df = tables[fact_table]

#         relationships = model_data.get("relationships", [])

#         for rel in relationships:
#             rel["from_column"] = self.normalize_column(rel["from_column"])
#             rel["to_column"] = self.normalize_column(rel["to_column"])

#         joined_tables = set()

#         # =====================================================
#         # 🔥 JOIN ENGINE WITH LINEAGE TRACKING
#         # =====================================================
#         for table_name, right_df in tables.items():

#             if table_name == fact_table:
#                 continue

#             if table_name in joined_tables:
#                 continue

#             joined = False

#             # AI JOIN
#             for rel in relationships:

#                 if rel.get("to_table") != table_name:
#                     continue

#                 left_col = self.find_column(df, rel["from_column"])
#                 right_col = self.find_column(right_df, rel["to_column"])

#                 if left_col and right_col:
#                     df = self.safe_merge(df, right_df, left_col, right_col, table_name)

#                     # 🔥 UPDATE LINEAGE AFTER MERGE
#                     for col in df.columns:
#                         if col not in column_lineage:
#                             column_lineage[col] = table_name

#                     joined = True
#                     joined_tables.add(table_name)
#                     break

#             # AUTO JOIN
#             # AUTO JOIN (SMART)
#             if not joined:

#                 left_col, right_col = self.infer_join(df, right_df)

#                 if left_col and right_col:
#                     new_df = self.safe_merge(df, right_df, left_col, right_col, table_name)

#                     # Only accept if columns increased
#                     if len(new_df.columns) > len(df.columns):
#                         df = new_df

#                         for col in df.columns:
#                             if col not in column_lineage:
#                                 column_lineage[col] = table_name

#                         joined_tables.add(table_name)

#         # FINAL CLEAN
#         df = self.clean_columns(df)

#         # 🔥 FINAL SAFETY: ENSURE ALL COLUMNS HAVE LINEAGE
#         for col in df.columns:
#             if col not in column_lineage:
#                 column_lineage[col] = "unknown"

#         # SAVE
#         os.makedirs("output", exist_ok=True)
#         output_path = f"output/final_dataset_{job_id}.csv"

#         df.to_csv(output_path, index=False)
#         preview = df.head(10).to_dict(orient="records")

#         return {
#             "file": output_path,
#             "rows": len(df),
#             "columns": list(df.columns),
#             "preview": preview,
#             "lineage": column_lineage   # 🔥 CRITICAL
#         }


import pandas as pd
import re
from io import BytesIO
from azure.storage.blob import BlobServiceClient
from config import BLOB_CONN_STR, DATA_INGESTION_CONTAINER


class ETLService:

    def __init__(self):
        self.blob = BlobServiceClient.from_connection_string(BLOB_CONN_STR)

    # =========================
    # LOGGER
    # =========================
    def log(self, msg):
        print(f"[ETL] {msg}")

    # =========================
    # NORMALIZE COLUMN
    # =========================
    def normalize(self, col):
        return re.sub(r'\W+', '_', str(col).lower())

    # =========================
    # FIND COLUMN
    # =========================
    def find_column(self, df, target_col):

        target = self.normalize(target_col)

        for col in df.columns:
            if self.normalize(col) == target:
                return col

        return None

    # =========================
    # LOAD CSV (🔥 FIXED - NO TMP FILE)
    # =========================
    def load(self, user_id, job_id, table):

        path = f"{user_id}/{job_id}/{table}.csv"

        blob = self.blob.get_blob_client(
            container=DATA_INGESTION_CONTAINER,
            blob=path
        )

        data = blob.download_blob().readall()

        # ✅ Read directly from memory
        df = pd.read_csv(BytesIO(data))

        # normalize columns
        df.columns = [self.normalize(c) for c in df.columns]

        return df

    # =========================
    # PREFIX COLUMNS
    # =========================
    def prefix_columns(self, df, table_name, join_key):

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

            rename_map[col] = f"{prefix}_{col}"

        df.rename(columns=rename_map, inplace=True)

        return df

    # =========================
    # SAFE MERGE
    # =========================
    def safe_merge(self, left, right, lkey, rkey, table_name):

        if lkey is None or rkey is None:
            self.log(f"⚠️ Skipping join (missing key): {lkey}, {rkey}")
            return left

        if lkey not in left.columns or rkey not in right.columns:
            self.log(f"⚠️ Key not found: {lkey}, {rkey}")
            return left

        right = self.prefix_columns(right, table_name, rkey)

        before = len(left)

        merged = left.merge(right, left_on=lkey, right_on=rkey, how="left")

        after = len(merged)

        # prevent explosion
        if after > before * 2:
            self.log("⚠️ Merge explosion detected, skipping")
            return left

        return merged

    # =========================
    # EXECUTE
    # =========================
    def execute(self, dataset, user_id, job_id, model_output):

        model = model_output["model_output"]["data"]

        tables = {}
        relationships = model["relationships"]
        fact = model["model"]["fact_table"]

        # =====================
        # LOAD TABLES
        # =====================
        for t in model["tables"]:
            name = t["table_name"]
            try:
                tables[name] = self.load(user_id, job_id, name)
            except Exception as e:
                self.log(f"⚠️ Failed loading {name}: {e}")
                continue

        self.log(f"Tables: {list(tables.keys())}")
        self.log(f"Fact: {fact}")
        self.log(f"Relationships: {relationships}")

        df = tables[fact]
        joined = set()

        # =====================
        # FACT → DIM
        # =====================
        for rel in relationships:

            if rel["from_table"] != fact:
                continue

            right = rel["to_table"]

            if right not in tables:
                continue

            lkey = self.find_column(df, rel["from_column"])
            rkey = self.find_column(tables[right], rel["to_column"])

            self.log(f"Joining {fact} → {right} on {lkey}={rkey}")

            df = self.safe_merge(df, tables[right], lkey, rkey, right)

            joined.add(right)

        # =====================
        # CONTROLLED DIM JOIN (FIXED)
        # =====================
        queue = list(joined)   # start with fact-connected tables

        while queue:
            current = queue.pop(0)

            for rel in relationships:

                if rel["from_table"] != current:
                    continue

                right = rel["to_table"]

                if right in joined:
                    continue

                if right not in tables:
                    continue

                lkey = self.find_column(df, rel["from_column"])
                rkey = self.find_column(tables[right], rel["to_column"])

                self.log(f"Joining {current} → {right} on {lkey}={rkey}")

                df = self.safe_merge(df, tables[right], lkey, rkey, right)

                joined.add(right)
                queue.append(right)

        # =====================
        # FINAL OUTPUT (🔥 FIXED - NO LOCAL FILE)
        # =====================
        self.log(f"Final rows: {len(df)}")
        self.log(f"Final columns: {len(df.columns)}")

        return {
            "rows": len(df),
            "columns": list(df.columns),
            "preview": df.head(10).to_dict(orient="records")
        }
