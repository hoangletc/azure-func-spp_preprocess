import datetime
import json
import re

import azure.functions as func
import pytz

from . import process, utils

RES_MAPPING = {
    "BI_MATU": 'material_use_trans',
    "BI_ASSET": "asset",
    "BI_INVE": "inventory",
    "BI_ITEM": "item",
    "BI_MATR": "material_receipt_trans",
    "BI_WO": "work_order",
    "BI_SERV": "services",
    "BI_INVT": "inventory_trans",
    "BI_INVB": "inventory_balance",
    "BI_LOC": "location"
}

PATH_RAW = "spp/cmms_data_dev/raw"
PATH_PROCESSED = "spp/cmms_data_dev/processed"


def main(req: func.HttpRequest) -> func.HttpResponse:
    req_body = req.get_json()

    # Extract requested data
    if isinstance(req_body, str):
        data = json.loads(req_body)
    elif isinstance(req_body, dict):
        data = req_body

    # Extract res_name
    if 'responseInfo' not in data or 'href' not in data['responseInfo']:
        return func.HttpResponse("Field 'responseInfo' or 'responseInfo['href']' error", status_code=500)

    res_name_ = re.findall(r"\/(BI_\w+)", data['responseInfo']['href'])[0]
    res_name = RES_MAPPING[res_name_]

    # Process data
    processed = process.preprocess(data, res_name)

    # Create 'filename'
    a = datetime.datetime.now(pytz.timezone('Asia/Ho_Chi_Minh'))
    dt_s = a.strftime("%Y%m%d-%H%M%S")
    filename = f"{res_name}_{dt_s}.json"

    # Save raw data
    path = f"{PATH_RAW}/{res_name}/{filename}"
    utils.save_file(data, filename, path)

    # Save processed data
    for res_name, dat in processed.items():
        path = f"{PATH_PROCESSED}/{res_name}/{filename}"
        utils.save_file(dat, filename, path)

    return func.HttpResponse("Stored at", status_code=200)
