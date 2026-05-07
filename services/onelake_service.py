import requests

from azure.identity import ClientSecretCredential

from azure.storage.filedatalake import DataLakeServiceClient

from azure.core.exceptions import ResourceNotFoundError, HttpResponseError

import time
 
from config import (

    TENANT_ID,

    CLIENT_ID,

    CLIENT_SECRET,

    FABRIC_WORKSPACE_ID,

    FABRIC_LAKEHOUSE_ID

)
 
 
class OneLakeService:
 
    def __init__(self):

        self.credential = ClientSecretCredential(

            tenant_id=TENANT_ID,

            client_id=CLIENT_ID,

            client_secret=CLIENT_SECRET

        )
 
        self.account_url = "https://onelake.dfs.fabric.microsoft.com"
 
        self.service_client = DataLakeServiceClient(

            account_url=self.account_url,

            credential=self.credential

        )
 
        # ✅ Filesystem = WORKSPACE ID (not Lakehouse ID)

        self.fs_client = self.service_client.get_file_system_client(FABRIC_WORKSPACE_ID)
 
        # ✅ All file paths are prefixed with this

        self.lakehouse_root = f"{FABRIC_LAKEHOUSE_ID}.Lakehouse"
 
    # =========================

    # TOKEN (for upload)

    # =========================

    def get_token(self):

        token = self.credential.get_token("https://storage.azure.com/.default")

        return token.token
 
    # =========================

    # UPLOAD FILE (Blob endpoint)

    # =========================

    def upload_file(self, file_buffer, user_id, job_id, dataset_name):

        token = self.get_token()
 
        data = file_buffer if isinstance(file_buffer, bytes) else file_buffer.getvalue()
 
        # Short path stored in job JSON

        short_path = f"Files/Datasets/{user_id}/{job_id}/{dataset_name}"
 
        url = (

            f"https://onelake.blob.fabric.microsoft.com"

            f"/{FABRIC_WORKSPACE_ID}/{FABRIC_LAKEHOUSE_ID}/{short_path}"

        )
 
        headers = {

            "Authorization": f"Bearer {token}",

            "x-ms-blob-type": "BlockBlob",

            "Content-Type": "application/octet-stream"

        }
 
        response = requests.put(url, headers=headers, data=data)
 
        if response.status_code not in [200, 201]:

            raise Exception(f"OneLake upload failed: {response.text}")
 
        return short_path
 
    # =========================

    # RENAME FILE (DFS endpoint)

    # =========================

    def rename_file(self, user_id, job_id, old_name, new_name):

        """

        Atomically renames a file in OneLake using ADLS Gen2 DFS rename.

        Filesystem = WorkspaceID, path includes {LakehouseID}.Lakehouse prefix.

        """
 
        # ✅ Full DFS paths (workspace-relative)

        old_full_path = f"{self.lakehouse_root}/Files/Datasets/{user_id}/{job_id}/{old_name}"

        new_full_path = f"{self.lakehouse_root}/Files/Datasets/{user_id}/{job_id}/{new_name}"
 
        # ✅ Rename destination = {workspace_id}/{new_full_path}

        rename_destination = f"{FABRIC_WORKSPACE_ID}/{new_full_path}"
 
        print(f"\n🔁 OneLake DFS Rename")

        print(f"   WORKSPACE  : {FABRIC_WORKSPACE_ID}")

        print(f"   FILESYSTEM : {FABRIC_WORKSPACE_ID}  (workspace, not lakehouse)")

        print(f"   FROM       : {old_full_path}")

        print(f"   TO         : {new_full_path}")

        print(f"   DEST       : {rename_destination}")
 
        file_client = self.fs_client.get_file_client(old_full_path)
 
        # Verify source exists before attempting rename

        try:

            file_client.get_file_properties()

            print("✅ Source file confirmed in OneLake")

        except ResourceNotFoundError:

            print(f"⚠️ Source not found at: {old_full_path}")

            raise
 
        # Attempt rename with retries

        for attempt in range(3):

            try:

                file_client.rename_file(rename_destination)

                print("✅ OneLake rename successful")

                return f"Files/Datasets/{user_id}/{job_id}/{new_name}"

            except HttpResponseError as e:

                print(f"⚠️ Rename attempt {attempt + 1} failed: {e.message}")

                time.sleep(2)
 
        raise Exception("OneLake rename failed after 3 attempts")
 
