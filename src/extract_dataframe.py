import pandas as pd
from io import BytesIO
import requests

def extract_and_save_xml(url, output_path):
    """
    Função para baixar um arquivo XML de uma URL, carregar em um DataFrame do Pandas
    e salvar o DataFrame como um arquivo Parquet.

    Args:
    - url: URL do arquivo XML
    - output_path: Caminho de saída para salvar o arquivo Parquet
    """
    # Baixe o arquivo XML
    response = requests.get(url)

    if response.status_code == 200:
        # Carregue o XML em um DataFrame do Pandas
        dataframe = pd.read_xml(BytesIO(response.content), encoding='ISO-8859-1')

        try:
            # Salve o DataFrame como arquivo Parquet
            dataframe.to_parquet(output_path)
            print(f"DataFrame salvo como arquivo Parquet em {output_path}")
        except Exception as e:
            print(f"Erro ao salvar o DataFrame como Parquet: {e}")
    else:
        print("Erro ao baixar o arquivo XML")

# Remova o exemplo de uso da função, pois ele será chamado pela DAG do Airflow.