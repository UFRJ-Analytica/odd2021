from os import makedirs, listdir, environ
import json

import pandas as pd
import basedosdados as bd
from dotenv import load_dotenv

def get_data(filename, extension="csv", data_path="./Dados"):
    """Gets data from specified data_path or `basedosdados API`

    Args:
        filename (str): The name of the file you want to get
        extension (str): The extension of the file you want to get [csv/json]
        data_path (str): data_path to the data folder
    """

    # Checks for valid extension arg and creates file helper variables
    if extension not in ["csv", "json"]: return ValueError("Invalid Extension")
    file = f"{filename}.{extension}"
    file_path = f"{data_path}/{file}"

    # Checks if data_path exists, if not creates it
    makedirs(data_path, exist_ok=True)

    # Checks for file existence in data_path. Loads it if found or creates it
    if file in listdir(data_path):
        if extension == "csv":
            try:
                df = pd.read_csv(file_path)
                return df
            except Exception as e:
                print(e)
                return None
        elif extension == "json":
            try:
                with open(file_path, mode="r") as f:
                    return json.loads(f)
            except Exception as e:
                print(e)
                return None
    else:
        load_dotenv("./.env")
        dataset_id, table_id = filename.split(".")
        df = bd.read_table(dataset_id = dataset_id, table_id = table_id, billing_project_id = environ.get("PROJECT_ID"))
        if extension == "csv":
            df.to_csv(file_path, index=False)
            return df
        elif extension == "json":
            result = df.to_json(orient="index")
            parsed = json.loads(result)
            try:
                with open(file_path, mode="w") as f:
                    json.dumps(parsed, f)
            except Exception as e:
                print(e)
            return parsed