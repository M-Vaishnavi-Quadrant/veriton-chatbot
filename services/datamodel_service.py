import io
import pandas as pd
from azure.storage.blob import BlobServiceClient
from config import BLOB_CONN_STR, DATA_INGESTION_CONTAINER


class DataModelService:

    def __init__(self):
        self.blob = BlobServiceClient.from_connection_string(BLOB_CONN_STR)

    def log(self, msg):
        print(f"[MODEL] {msg}")

    def normalize(self, col):
        return col.lower().replace(" ", "_")

    # =========================
    # LOAD TABLES
    # =========================
    def load_tables(self, user_id, job_id):

        container = self.blob.get_container_client(DATA_INGESTION_CONTAINER)
        prefix = f"{user_id}/{job_id}/"

        tables = {}

        for blob in container.list_blobs(name_starts_with=prefix):

            if not blob.name.endswith(".csv"):
                continue

            name = blob.name.split("/")[-1].replace(".csv", "")

            data = container.get_blob_client(blob.name).download_blob().readall()

            tables[name] = pd.read_csv(io.BytesIO(data))

        return tables

    # =========================
    # RELATIONSHIP DETECTION (DYNAMIC)
    # =========================
    def detect_relationships(self, tables):

        relationships = []

        for t1, df1 in tables.items():
            for t2, df2 in tables.items():

                if t1 == t2:
                    continue

                for c1 in df1.columns:

                    col1 = self.normalize(c1)

                    # =========================
                    # RULE 1: FK NAMING MATCH
                    # =========================
                    if not col1.endswith("_id"):
                        continue

                    # =========================
                    # RULE 2: SAME COLUMN EXISTS
                    # =========================
                    for c2 in df2.columns:

                        col2 = self.normalize(c2)

                        if col1 != col2:
                            continue

                        try:
                            s1 = set(df1[c1].dropna())
                            s2 = set(df2[c2].dropna())

                            if not s1 or not s2:
                                continue

                            # =========================
                            # RULE 3: FK → PK BEHAVIOR
                            # =========================
                            uniq1 = len(s1) / len(df1)   # FK side (low uniqueness)
                            uniq2 = len(s2) / len(df2)   # PK side (high uniqueness)

                            overlap = len(s1 & s2) / len(s1)

                            if (
                                overlap > 0.2 and      # relaxed
                                uniq2 > uniq1          # PK side more unique
                            ):
                                relationships.append({
                                    "from_table": t1,
                                    "to_table": t2,
                                    "from_column": c1,
                                    "to_column": c2
                                })

                        except:
                            continue

        return relationships

    # =========================
    # CLEAN RELATIONSHIPS
    # =========================
    def clean_relationships(self, relationships, fact_table):

        cleaned = []
        seen = set()

        for rel in relationships:

            f = rel["from_table"]
            t = rel["to_table"]

            if f == t:
                continue

            key = tuple(sorted([f, t]))

            if key in seen:
                continue

            seen.add(key)
            cleaned.append(rel)

        final = []

        for rel in cleaned:
            if rel["from_table"] == fact_table:
                final.append(rel)

        level1 = {r["to_table"] for r in final}

        for rel in cleaned:
            if rel["from_table"] in level1:
                final.append(rel)

        return final

    # =========================
    # FACT DETECTION
    # =========================
    def detect_fact(self, relationships, tables):

        score = {}

        for rel in relationships:
            score[rel["from_table"]] = score.get(rel["from_table"], 0) + 1

        if score:
            return max(score, key=score.get)

        return list(tables.keys())[0]

    # =========================
    # EXECUTE
    # =========================
    def execute(self, user_id, job_id):

        tables = self.load_tables(user_id, job_id)

        relationships = self.detect_relationships(tables)

        fact = self.detect_fact(relationships, tables)

        relationships = self.clean_relationships(relationships, fact)

        schemas = {t: list(df.columns) for t, df in tables.items()}

        self.log(f"Fact: {fact}")
        self.log(f"Relationships: {len(relationships)}")

        return {
            "model_output": {
                "data": {
                    "model": {
                        "fact_table": fact,
                        "dimension_tables": [t for t in tables if t != fact]
                    },
                    "relationships": relationships,
                    "schemas": schemas,
                    "tables": [{"table_name": t} for t in tables]
                }
            }
        }
