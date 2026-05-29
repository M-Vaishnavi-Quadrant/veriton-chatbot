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
    # RENAME FILE
    # =========================

    def rename_file(

        self,

        old_path,

        new_path
    ):

        """
        Reliable OneLake rename using:
        DOWNLOAD -> UPLOAD -> DELETE
        """

        try:

            print("\n🔁 ONELAKE RENAME STARTED")

            print("OLD PATH:", old_path)

            print("NEW PATH:", new_path)

            # ==========================================
            # OLD FULL PATH
            # ==========================================

            old_full_path = (
                f"{self.lakehouse_root}/"
                f"{old_path}"
            )

            # ==========================================
            # DOWNLOAD OLD FILE
            # ==========================================

            old_client = (
                self.fs_client
                .get_file_client(
                    old_full_path
                )
            )

            file_data = (
                old_client
                .download_file()
                .readall()
            )

            print(
                "✅ Downloaded old file"
            )

            # ==========================================
            # UPLOAD NEW FILE
            # ==========================================

            token = self.get_token()

            upload_url = (

                f"https://onelake.blob.fabric.microsoft.com"

                f"/{FABRIC_WORKSPACE_ID}"

                f"/{FABRIC_LAKEHOUSE_ID}"

                f"/{new_path}"
            )

            headers = {

                "Authorization": (
                    f"Bearer {token}"
                ),

                "x-ms-blob-type": "BlockBlob",

                "Content-Type": (
                    "application/octet-stream"
                )
            }

            response = requests.put(

                upload_url,

                headers=headers,

                data=file_data
            )

            if response.status_code not in [200, 201]:

                raise Exception(

                    f"OneLake upload failed: "
                    f"{response.text}"
                )

            print(
                "✅ Uploaded renamed file"
            )

            # ==========================================
            # DELETE OLD FILE
            # ==========================================

            old_client.delete_file()

            print(
                "✅ Deleted old file"
            )

            return new_path

        except Exception as e:

            print(
                "❌ ONELAKE RENAME FAILED:",
                str(e)
            )

            raise Exception(
                f"OneLake rename failed: {str(e)}"
            )

