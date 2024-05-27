from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from plugins.extractapioperator import ExtractApiDataframeOperator
from datetime import datetime

default_args = {
    "owner": "my eggs",
    "start_date": datetime(2024, 5, 20)
}

dag = DAG(
    dag_id="extract_api_dataframe_dag",
    default_args=default_args,
    schedule_interval="None",
)

start_task = DummyOperator(task_id='start', dag=dag)

extract_task = ExtractApiDataframeOperator(
        task_id = "my_task",
        url="http://servicos.cptec.inpe.br/XML/capitais/condicoesAtuais.xml",
        output_path= "output/saida.parquet",
        dag=dag

)

end_task = DummyOperator(task_id='end', dag=dag)

start_task >> extract_task >> end_task

