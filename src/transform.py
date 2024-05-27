import pandas as pd
import numpy as np
from parser import aeroportos

dataframe_api = pd.read_parquet(r"D:\REPO ANTIGOS\rag_dev\output\saida.parquet")
dataframe_dict = pd.DataFrame.from_dict(aeroportos)

dataframe_dict = dataframe_dict.transpose()
dataframe_dict.reset_index(inplace=True)
dataframe_dict.drop("index", axis=1, inplace=True)
dataframe_dict = dataframe_dict.rename(columns={'Sigla': 'codigo'})
dataframe_dict["codigo"] = dataframe_dict["codigo"].astype("str")

dataframe_api["tempo_desc"] = dataframe_api["tempo_desc"].replace('PredomÃ­nio de Sol', 'Predominio de sol')
for col in dataframe_api.columns:
    if (dataframe_api[col] == 9999).any():
        dataframe_api.replace(9999, np.nan, inplace=True)
dataframe_api.dropna(inplace=True)
dataframe_api["codigo"] = dataframe_api["codigo"].astype("str")



dataframe_aero = pd.concat([dataframe_api, dataframe_dict])



try:
    dataframe_aero.to_parquet(r"D:\REPO ANTIGOS\rag_dev\output\saida_transformada.parquet")
    print(f"DataFrame salvo como arquivo Parquet")
except Exception as e:
    print(f"Erro ao salvar o DataFrame como Parquet: {e}")