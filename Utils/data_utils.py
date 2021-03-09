from os import makedirs, listdir, environ
import json

import pandas as pd
import basedosdados as bd
from dotenv import load_dotenv

def get_data(filename, extension="csv", data_path="./Dados", query=None, save=True):
    """Gets data from specified data_path or basedosdados API.

    Args:
        filename (str): The name of the file you want to get
        extension (str, optional): The extension of the file you want to get [csv/json]. Defaults to "csv".
        data_path (str, optional): The path to the data folder. Defaults to "./Dados".
        query (str, optional): A query to execute retrieving data. Defaults to None.
        save (bool, optional): If the file should be saved. Defaults to True.
    Returns:
        DataFrame: Returns a DataFrame if `csv` is the chosen extension.
        dict: Returns a dict if `json` is the chosen extension.
    """

    # Checks for valid extension arg and creates file helper variables
    if extension not in ["csv", "json"]: return ValueError("Invalid Extension")
    file = f"{filename}.{extension}"
    file_path = f"{data_path}/{file}"

    # Checks if data_path exists, if not creates it
    makedirs(data_path, exist_ok=True)

    # Checks for file existence in data_path. Loads it if found or creates it
    if file in listdir(data_path) and query==None:
        if extension == "csv":
            try:
                df = pd.read_csv(file_path, sep=";")
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
        
        if query == None:
            dataset_id, table_id = filename.split(".")
            df = bd.read_table(dataset_id = dataset_id, table_id = table_id, billing_project_id = environ.get("PROJECT_ID"))
        else:
            df = bd.read_sql(query = query, billing_project_id = environ.get("PROJECT_ID"))

        if extension == "csv":
            if save:
                df.to_csv(file_path, sep=";", index=False)
            return df
        elif extension == "json":
            result = df.to_json(orient="index")
            parsed = json.loads(result)
            if save:
                try:
                    with open(file_path, mode="w") as f:
                        json.dumps(parsed, f)
                except Exception as e:
                    print(e)
            return parsed


def get_mun_ids(as_dict=False , data_path="./Dados"):
    """Gets municipalities ids and its additional informations.

    Args:
        as_dict (bool, optional): If the file must be returned as dict. Defaults to False.
        data_path (str, optional): The path to the data folder. Defaults to "./Dados".

    Returns:
        DataFrame: Returns a DataFrame containing the municipality id and its additional informations if as_dict is False.
        dict: Returns a dict containing the municipality id as key and an dict with the additional informations as value if as_dict is True.
    """
    try:
        if as_dict:
            with open(f"{data_path}/id_municipios.json", mode="r") as f:
                return json.load(f)
        else:
            return pd.read_csv(f"{data_path}/id_municipios.csv", sep=";").set_index("id_municipio")

    except FileNotFoundError:
        id_mun = get_data("", query="SELECT DISTINCT id_municipio, municipio, microrregiao, mesorregiao, sigla_uf, uf, regiao FROM `basedosdados.br_bd_diretorios_brasil.municipio`", save=False)

        if as_dict:
            dic = {}

            for _, row in id_mun.iterrows():
                dic[row["id_municipio"]] = {
                    "municipio" : row["municipio"],
                    "microrregiao" : row["microrregiao"],
                    "mesorregiao" : row["mesorregiao"],
                    "sigla_uf" : row["sigla_uf"],
                    "uf" : row["uf"],
                    "regiao" : row["regiao"]
                }

            with open(f"{data_path}/id_municipios.json", mode="w") as f:
                json.dump(dic, f)
            
            return dic
        else:
            id_mun.to_csv(f"{data_path}/id_municipios.csv", sep=";", index=False)
            return id_mun.set_index("id_municipio")


def get_br_ms_sim_ids(as_dict=False, data_path="./Dados"):
    """Gets br_ms_sim's causa_basica ids.

    Args:
        as_dict (bool, optional): If the file must be returned as dict. Defaults to False.
        data_path (str, optional): The path to the data folder. Defaults to "./Dados".

    Returns:
        DataFrame: Returns a DataFrame containing br_ms_sim's causa_basica ids if as_dict is False.
        dict: Returns a dict containing br_ms_sim's causa_basica id as key and its description as value if as_dict is True.
    """
    try:
        if as_dict:
            with open(f"{data_path}/CID-10-BR_MS_SIM.json", mode="r") as f:
                return json.load(f)
        else:
            return pd.read_csv(f"{data_path}/CID-10-BR_MS_SIM.csv", sep=";").set_index("causa_basica")

    except FileNotFoundError:
        raw_file = pd.read_csv("https://raw.githubusercontent.com/basedosdados/mais/master/bases/br_ms_sim/dictionaries/CID10/CID-10-SUBCATEGORIAS.CSV", sep=";", encoding="ANSI")
        df = raw_file[["SUBCAT","DESCRICAO"]].rename(columns={"SUBCAT": "causa_basica", "DESCRICAO": "descricao"})
        if as_dict:
            dic = {}

            for _, row in df.iterrows():
                dic[row["causa_basica"]] = row["descricao"]

            with open(f"{data_path}/CID-10-BR_MS_SIM.json", mode="w") as f:
                json.dump(dic, f)
            
            return dic
        else:
            df.to_csv(f"{data_path}/CID-10-BR_MS_SIM.csv", sep=";", index=False)
            return df.set_index("causa_basica")