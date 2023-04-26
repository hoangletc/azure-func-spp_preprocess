import json
from typing import Union

from azure.storage.blob import BlobServiceClient

# CONNECTION_STR = "DefaultEndpointsProtocol=https;AccountName=spvbstoragedevv2;AccountKey=mnql8TSM53Myn/rHlSiVMTSpXz9zL1oUnv3U8tIvtVIsHRELVjMPjwRU2qj58V7w+zevlopk8X2vrqxqb+OSUA==;EndpointSuffix=core.windows.net"


def save_file(data: Union[dict, list], filename: str, path_store: str, conf: dict):

    # Establish connection
    assert "DefaultEndpointsProtocol" in conf
    assert "AccountName" in conf
    assert "AccountKey" in conf
    assert "EndpointSuffix" in conf

    conn_str = ';'.join([
        f"DefaultEndpointsProtocol={conf['DefaultEndpointsProtocol']}",
        f"AccountName={conf['AccountName']}",
        f"AccountKey={conf['AccountKey']}",
        f"EndpointSuffix={conf['EndpointSuffix']}"
    ])
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    blob_client = blob_service_client.get_blob_client(
        container=path_store,
        blob=filename
    )

    # Convert str to binary
    data_encoded = bytes(json.dumps(data, indent=2, ensure_ascii=False), 'utf-8')

    # Write to Azure Blob
    blob_client.upload_blob(data_encoded, blob_type="BlockBlob", overwrite=True)
