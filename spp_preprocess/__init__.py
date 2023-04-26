import datetime
import json
import re
import secrets
import string
import traceback

import azure.functions as func
import pytz

from . import process, utils

# PATH_RAW = "spp/cmms_data_dev/raw"
PATH_PROCESSED = "spp/cmms_data_dev"


def gen_rand(n: int = 5):
    return ''.join(secrets.choice(string.ascii_uppercase + string.digits)
                   for i in range(n))


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()

        # Extract requested data
        if isinstance(req_body, str):
            data = json.loads(req_body)
        elif isinstance(req_body, dict):
            data = req_body

        # Extract res_name
        if 'responseInfo' not in data or 'href' not in data['responseInfo']:
            return func.HttpResponse("Field 'responseInfo' or 'responseInfo['href']' error", status_code=500)

        res_name = re.findall(r"\/(BI_\w+)", data['responseInfo']['href'])[0]

        # Process data
        processed = process.preprocess(data, res_name)

        # Create 'filename'
        a = datetime.datetime.now(pytz.timezone('Asia/Ho_Chi_Minh'))
        dt_d, dt_s = a.strftime("%Y%m%d"), a.strftime("%H%M%S")

        # Save raw data
        # path = f"{PATH_RAW}/{res_name}/{filename}"
        # utils.save_file(data, filename, path)

        # Save processed data
        for name, dat in processed.items():
            if name == "BI_ASSET_01":
                name = "BI_ASSET"
                filename = f"{dt_s}_{gen_rand()}.json"
            elif name == "BI_ASSET_02":
                name = "BI_ASSETSTATUS"
                filename = f"{dt_s}_{gen_rand()}.json"
            if name in ["BI_INVU_MATU", "BI_INVU_MATR"]:
                name = "BI_INVU"
                filename = f"{name[:4]}_{dt_s}_{gen_rand()}.json"
            elif name in ["BI_INVUL_MATU", "BI_INVUL_MATR"]:
                name = "BI_INVUL"
                filename = f"{name[:4]}_{dt_s}_{gen_rand()}.json"
            else:
                filename = f"{dt_s}_{gen_rand()}.json"

            path = f"{PATH_PROCESSED}/{name}/{dt_d}"

            utils.save_file(dat, filename, path)

        out = "OK"
        status_code = 200
    except Exception:
        out = traceback.format_exc()
        status_code = 500

    return func.HttpResponse(out, status_code=status_code, mimetype="application/json")
