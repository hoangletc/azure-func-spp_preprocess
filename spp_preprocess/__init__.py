import datetime
import json
import logging
import re

import azure.functions as func
import pytz
from azure.storage.blob import BlobServiceClient


def main(req: func.HttpRequest) -> func.HttpResponse:
    req_body = req.get_json()

    if isinstance(req_body, str):
        data = json.loads(req_body)
    elif isinstance(req_body, dict):
        data = req_body

    # Establish 'filename'
    if 'responseInfo' not in data or 'href' not in data['responseInfo']:
        return func.HttpResponse("Field 'responseInfo' or 'responseInfo['href']' error", status_code=500)

    res_name = re.findall(r"\/(BI_\w+)", data['responseInfo']['href'])[0]

    a = datetime.datetime.now(pytz.timezone('Asia/Ho_Chi_Minh'))
    dt_s = a.strftime("%Y%m%d-%H%M%S")

    filename = f"{res_name}_{dt_s}.json"

    # Establish connection
    connection_string = "DefaultEndpointsProtocol=https;AccountName=spvbstoragedevv2;AccountKey=mnql8TSM53Myn/rHlSiVMTSpXz9zL1oUnv3U8tIvtVIsHRELVjMPjwRU2qj58V7w+zevlopk8X2vrqxqb+OSUA==;EndpointSuffix=core.windows.net"
    container_name = f"spp/cmms_data_dev/{res_name}"
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(
        container=container_name,
        blob=filename
    )

    # Convert str to binary
    data_encoded = bytes(json.dumps(data, indent=2, ensure_ascii=False), 'utf-8')

    # Write to Azure Blob
    blob_client.upload_blob(data_encoded, blob_type="BlockBlob", overwrite=True)

    return func.HttpResponse("Stored at", status_code=200)
