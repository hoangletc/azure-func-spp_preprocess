import json
from typing import List

PATH_SCHEMA = "schemas.json"
with open(PATH_SCHEMA) as fp:
    SCHEMA = {k: set(v) for k, v in json.load(fp).items()}


# def parser(d: List[dict], fields: list, nested: bool = False, nested_field: str = None) -> list:
#     out = []

#     field_set = set(fields)
#     for x in d:
#         if nested is True:
#             assert nested_field is not None
#             x = x[nested_field]

#         entry = {**x}

#         existing_fields = set(x.keys())
#         nonexisted_fields = field_set.difference(existing_fields)
#         for f in nonexisted_fields:
#             entry[f] = None

#         out.append(entry)

#     return


def parser(name: str, d: List[dict], schema: dict) -> dict:
    def parse(r, field_set):
        entry = {**r}

        existing_fields = set(r.keys())
        nonexisted_fields = field_set.difference(existing_fields)
        for f in nonexisted_fields:
            entry[f] = None

        return entry

    if name == "BI_ASSET_01":
        name = "BI_ASSET"
    elif name == "BI_ASSET_02":
        name = "BI_ASSETSTATUS"

    out = {name: []}

    for x in d:
        if name == "BI_ASSET":

            p_asset = parse(x, schema[name])

            if "assetancestor" in x:
                if "BI_ASSETANCESTOR" not in out:
                    out['BI_ASSETANCESTOR'] = []
                if 'assetancestor' in p_asset:
                    del p_asset['assetancestor']

                for x1 in x['assetancestor']:
                    p = parse(x1, schema['BI_ASSETANCESTOR'])
                    p['assetuid'] = p_asset['assetuid']

                    out['BI_ASSETANCESTOR'].append(p)

            out[name].append(p_asset)
        if name == "BI_INVE":

            p_inve = parse(x, schema[name])

            if "invcost" in x:
                if "BI_INVCOST" not in out:
                    out['BI_INVCOST'] = []
                if 'invcost' in p_inve:
                    del p_inve['invcost']

                for x1 in x['invcost']:
                    p = parse(x1, schema['BI_INVCOST'])
                    p['inventoryid'] = p_inve['inventoryid']

                    out['BI_INVCOST'].append(p)

            out[name].append(p_inve)
        elif name == "BI_MATU":
            if "BI_INVU_MATU" not in out:
                out['BI_INVU_MATU'] = []
            if "BI_INVUL_MATU" not in out:
                out['BI_INVUL_MATU'] = []

            p_matu = parse(x, schema['BI_MATU'])

            if "invuse" in x:
                if 'invuse' in p_matu:
                    del p_matu['invuse']

                if isinstance(x['invuse'], dict):
                    x['invuse'] = [x['invuse']]
                for x1 in x['invuse']:
                    p = parse(x1, schema['BI_INVU'])
                    out['BI_INVU_MATU'].append(p)

            if "invuseline" in x:
                if 'invuseline' in p_matu:
                    del p_matu['invuseline']

                if isinstance(x['invuseline'], dict):
                    x['invuseline'] = [x['invuseline']]
                for x1 in x['invuseline']:
                    p = parse(x1, schema['BI_INVUL'])
                    out['BI_INVUL_MATU'].append(p)

            out[name].append(p_matu)

        elif name == "BI_MATR":
            if "BI_INVU_MATR" not in out:
                out['BI_INVU_MATR'] = []
            if "BI_INVUL_MATR" not in out:
                out['BI_INVUL_MATR'] = []

            p_matr = parse(x, schema['BI_MATR'])

            if "invuse" in x:
                if 'invuse' in p_matr:
                    del p_matr['invuse']

                if isinstance(x['invuse'], dict):
                    x['invuse'] = [x['invuse']]
                for x1 in x['invuse']:
                    p = parse(x1, schema['BI_INVU'])
                    out['BI_INVU_MATR'].append(p)
            if "invuseline" in x:
                if 'invuseline' in p_matr:
                    del p_matr['invuseline']

                if isinstance(x['invuseline'], dict):
                    x['invuseline'] = [x['invuseline']]
                for x1 in x['invuseline']:
                    p = parse(x1, schema['BI_INVUL'])
                    out['BI_INVUL_MATR'].append(p)

            out[name].append(p_matr)

        elif name == "BI_WO":
            if "BI_WOSTATUS" not in out:
                out['BI_WOSTATUS'] = []

            p_wo = parse(x, schema['BI_WO'])

            if "wostatus" in x:
                if 'wostatus' in p_wo:
                    del p_wo['wostatus']

                for x1 in x['wostatus']:
                    p = parse(x1, schema['BI_WOSTATUS'])
                    p['workorderid'] = p_wo['workorderid']

                    out['BI_WOSTATUS'].append(p)

            out[name].append(p_wo)

        else:
            p = parse(x, schema[name])
            out[name].append(p)

    return out

# def preprocess(data: dict, name: str) -> dict:
#     # NOTE: HoangLe [Apr-04]: This version will be used in optimizing phase

#     if name == "BI_ASSET":
#         if 'assetstatus' in data['member'][0]:
#             out = {
#                 'BI_ASSETSTATUS': parser(data['member'], SCHEMA['BI_ASSETSTATUS'],
#                                          True, 'assetstatus')
#             }
#         else:
#             out = {"BI_ASSET": parser(data['member'], SCHEMA['BI_ASSET'])}
#     elif name == "BI_WO":
#         if 'wostatus' in data['member'][0]:
#             out = {'BI_WOSTATUS': parser(data['member'], SCHEMA['BI_WOSTATUS'],
#                                          True, 'wostatus')}

#         else:
#             out = {'BI_WO': parser(data['member'], SCHEMA['BI_WO'])}
#     elif name in ['BI_MATU', 'BI_MATR']:
#         if 'invuse' in data['member'][0]:
#             out = {
#                 'BI_INVU': parser(data['member'], SCHEMA['BI_INVU'],
#                                   True, 'invuse')}
#         elif 'invuseline' in data['member'][0]:
#             out = {'BI_INVUL': parser(data['member'], SCHEMA['BI_INVUL'],
#                                       True, 'invuseline')}
#         else:
#             out = {name: parser(data['member'], SCHEMA[name])}
#     else:
#         out = {name: parser(data['member'], SCHEMA[name])}

#     return out


def preprocess(data: dict, name: str) -> dict:
    return parser(name, data['member'], SCHEMA)
