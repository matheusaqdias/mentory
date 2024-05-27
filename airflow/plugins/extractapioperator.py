from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from io import BytesIO
import requests
import pandas as pd

class ExtractApiDataframeOperator(BaseOperator):
    
    @apply_defaults
    def __init__(self, url, output_path, *args, **kwargs):
        """
        Inicialização do operador.

        Args:
            url (str): URL do arquivo XML
            output_path (str): Caminho de saída para salvar o arquivo Parquet
        """
        super().__init__(*args, **kwargs)
        self.url = url
        self.output_path = output_path

    def execute(self, context):
        """
        Método chamado quando a tarefa é executada.

        Aqui você pode chamar sua função `extract_and_save_xml`.
        """
        try:
            # Baixe o arquivo XML
            self.log.info(f"Baixando o arquivo XML da URL: {self.url}")
            response = requests.get(self.url)

            if response.status_code == 200:
                # Carregue o XML em um DataFrame do Pandas
                self.log.info("Convertendo o XML em DataFrame")
                dataframe = pd.read_xml(BytesIO(response.content), encoding='ISO-8859-1')

                # Salve o DataFrame como arquivo Parquet
                self.log.info(f"Salvando o DataFrame como arquivo Parquet em {self.output_path}")
                dataframe.to_parquet(self.output_path)
                self.log.info(f"DataFrame salvo como arquivo Parquet em {self.output_path}")
            else:
                self.log.error(f"Erro ao baixar o arquivo XML. Código de status: {response.status_code}")
        except Exception as e:
            self.log.error(f"Erro ao processar o arquivo XML: {e}")
