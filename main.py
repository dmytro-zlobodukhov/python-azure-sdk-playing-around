import os
from azure.identity import DefaultAzureCredential
from azure.mgmt.storage import StorageManagementClient
from azure.storage.blob import BlobServiceClient


RESOURCE_GROUP_NAME = os.environ.get('AZURE_RESOURCE_GROUP_NAME', "")
SUBSCRIPTION_ID = os.environ.get('AZURE_SUBSCRIPTION_ID', None)
STORAGE_ACCOUNT_NAME = os.environ.get('AZURE_STORAGE_ACCOUNT_NAME', "")
STORAGE_ACCOUNT_BLOB_CONTAINER_NAME = os.environ.get('AZURE_STORAGE_ACCOUNT_BLOB_CONTAINER_NAME', "")


def get_storage_accounts(subscription_id, resource_group_name):
    """List all storage accounts in the specified resource group."""

    credential = DefaultAzureCredential()
    storage_client = StorageManagementClient(credential, subscription_id)
    
    return storage_client.storage_accounts.list_by_resource_group(resource_group_name)


def get_storage_account_by_name(subscription_id, resource_group_name, storage_account_name):
    """Get the specified storage account in the specified resource group."""

    credential = DefaultAzureCredential()
    storage_client = StorageManagementClient(credential, subscription_id)
    
    return storage_client.storage_accounts.get_properties(resource_group_name, storage_account_name)


def get_blob_containers(storage_account_name):
    """List all blob containers in the specified storage account."""

    credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient(account_url=f"https://{storage_account_name}.blob.core.windows.net", credential=credential)
    
    return blob_service_client.list_containers()


def get_blob_container_by_name(storage_account_name, container_name):
    """Get the specified blob container in the specified storage account."""

    credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient(account_url=f"https://{storage_account_name}.blob.core.windows.net", credential=credential)
    
    return blob_service_client.get_container_client(container_name)


def get_storage_accounts_access_keys(subscription_id, resource_group_name, storage_account_name, key_number=0):
    """Get the access keys for the specified storage account."""

    credential = DefaultAzureCredential()
    storage_client = StorageManagementClient(credential, subscription_id)
    key_result = storage_client.storage_accounts.list_keys(resource_group_name, storage_account_name)

    return key_result.keys[key_number].value


def list_blobs_in_container(storage_account_name, container_name):
    """List all blobs in the specified blob container."""

    credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient(account_url=f"https://{storage_account_name}.blob.core.windows.net", credential=credential)
    container_client = blob_service_client.get_container_client(container_name)
    blob_list = container_client.list_blobs()
    
    return blob_list


def upload_file_to_blob_container(storage_account_name, container_name, file_path, blob_name):
    """Upload a file to the specified blob container."""
    
    credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient(account_url=f"https://{storage_account_name}.blob.core.windows.net", credential=credential)
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)
    
    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    
    return blob_client


def download_file_from_blob_container(storage_account_name, container_name, blob_name, file_path):
    """Download a file from the specified blob container."""

    credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient(account_url=f"https://{storage_account_name}.blob.core.windows.net", credential=credential)
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)
    
    with open(file_path, "wb") as data:
        data.write(blob_client.download_blob().readall())
    
    return blob_client


def main():
    if SUBSCRIPTION_ID is None:
        raise ValueError("AZURE_SUBSCRIPTION_ID is not set")
    else:
        print(f"Using subscription id: {SUBSCRIPTION_ID}")

        # https://learn.microsoft.com/en-us/python/api/azure-mgmt-storage/azure.mgmt.storage.v2023_01_01.models.storageaccount?view=azure-python
        storage_account_name = get_storage_account_by_name(SUBSCRIPTION_ID, RESOURCE_GROUP_NAME, STORAGE_ACCOUNT_NAME).name

        # https://learn.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.containerclient?view=azure-python
        blob_container_name = get_blob_container_by_name(STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_BLOB_CONTAINER_NAME).container_name

        # Uploading local files to blob container
        for i in range(5):
            local_file_name = "./test.txt"
            blob_name = f"uploaded_test00{i}.txt"
            os.system(f"echo 'Hello, World! {i}' > {local_file_name}")
            result = upload_file_to_blob_container(storage_account_name, blob_container_name, local_file_name, blob_name)
            print(f"File '{result.blob_name}' successfully uploaded to '{result.container_name}' container in '{result.account_name}' storage account")

        # Listing all blobs in blob container
        blobs = list_blobs_in_container(storage_account_name, blob_container_name)
        print(f"List of blobs in '{blob_container_name}' container:")
        for blob in blobs:
            print(f" - {blob.name}")
        
        # Downloading local files from blob container
        for i in range(5):
            blob_name = f"uploaded_test00{i}.txt"
            local_file_name = f"./downloaded_test00{i}.txt"
            result = download_file_from_blob_container(storage_account_name, blob_container_name, blob_name, local_file_name)
            print(f"File '{result.blob_name}' successfully downloaded from '{result.container_name}' container in '{result.account_name}' storage account")


if __name__ == "__main__":
    main()