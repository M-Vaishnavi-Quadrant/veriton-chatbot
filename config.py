import os
from dotenv import load_dotenv

load_dotenv()   # 👈 THIS IS THE MISSING PIECE

# Storage
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")

BLOB_CONN_STR = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)

AI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AI_API_KEY  = os.getenv("AZURE_OPENAI_API_KEY")
AI_MODEL    = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o-mini")

# Containers
DATA_INGESTION_CONTAINER = "dataingestionfiles"
USER_CONTAINER = "userdata"
AZURE_CONTAINER="financedata"
DATASET_CONTAINER = "datasets"
METADATA_CONTAINER = "metadata"
RELATIONSHIP_CONTAINER = "relationships"
NORMALIZED_CONTAINER = "normalized"