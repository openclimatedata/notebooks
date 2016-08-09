# encoding: UTF-8

import os

import pandas as pd

from datapackage import DataPackage


def read_datapackage(url_or_path, resource_name=None):
    """
    TODO
    """
    if os.path.exists(url_or_path):
        if not url_or_path.endswith("datapackage.json"):
            url_or_path = os.path.join(url_or_path, "datapackage.json")
    elif url_or_path.startswith("https://github.com/"):
        url_or_path = "https://raw.githubusercontent.com/" + \
                       url_or_path.split("https://github.com/")[1] +\
                       "/master/datapackage.json"

    dp = DataPackage(url_or_path)

    if type(resource_name) is str:
        resource_name = [resource_name]

    resources = [r for r in dp.resources]
    if resource_name is not None:
        resources = [r for r in resources
                     if r.descriptor["name"] in resource_name]

    data_frames = {}

    for idx, resource in enumerate(resources):
        descriptor = resource.descriptor
        if "name" in descriptor:
            name = descriptor["name"]
        else:
            name = str(idx)
        index_col = None
        converters = {}
        parse_dates = []

        int_columns = []

        csv_path = resource.remote_data_path or resource.local_data_path
        if not csv_path.endswith(".csv"):
            continue

        if "primaryKey" in descriptor["schema"]:
            index_col = descriptor["schema"]["primaryKey"]

        for column in descriptor["schema"]["fields"]:
            if column["type"] == "int":
                int_columns.append(column["name"])
            elif column["type"] == "date":
                parse_dates.append(column["name"])

        if len(parse_dates) == 0:
            parse_dates = None

        df = pd.read_csv(
            csv_path,
            index_col=index_col,
            converters=converters,
            parse_dates=parse_dates
        )
        # Convert integer columns with missing values to type 'object'
        for int_col in int_columns:
            if df[int_col].isnull().sum() > 0:
                df[int_col] = df[int_col].astype(object)

        data_frames[name] = df

    if len(list(data_frames.values())) == 1:
        return list(data_frames.values())[0]
    else:
        return data_frames
