from azure. storage.blob import BlobServiceClient
pip install azure-storage-blob

storage_account_key = 'YpnBcljrLkmtjxd3sghmHtbg71wh2/KqKZdMfvdaDV3G5K/95xsR5s+nrOh+1EGpb7kJA6Zewm4L+AStYLYdDg=='
storage_account_name = 'cryptoadlstest'
connection_string = "DefaultEndpointsProtocol=https;AccountName=cryptoadlstest;AccountKey=3e/U3fYT7/LWq7rQbXflO0uHsm0II3oJzL1H0p7k1YNadk6CiASZ8KViAOkvU23KczQFm39LkxY9+ASt0DfF7g==;EndpointSuffix=core.windows.net"
container_name = 'output'


def uploadToBlobStorage(file_path, file_name):
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)

    with open(file_path, "rb") as data:
        blob_client.upload_blob(data)
    print("uploaded file_name" + file_name + "file")


uploadToBlobStorage('/output/part-00000-93f0c6a2-f50f-479f-91ad-ffbc90d23ae9-c000.json', 'data.json')