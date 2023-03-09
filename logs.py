from airflow import DAG
from datetime import datetime

from airflow.operators.python import PythonOperator


def send_logs():
    # Coloque aqui suas credenciais do Watson Assistant V2 e o ID da sua workspace
    # watson_apikey = "SUA_APIKEY_DO_WATSON_ASSISTANT_V2"
    # watson_url = "URL_DO_WATSON_ASSISTANT_V2"
    # workspace_id = "ID_DA_SUA_WORKSPACE_DO_WATSON_ASSISTANT_V2"

    # Definindo as datas para consulta dos logs do Watson Assistant
    # now = datetime.now()
    # end_time = now.isoformat() + "Z"  # Data e hora atual
    # start_time = (now - timedelta(hours=1)).isoformat() + "Z"  # Há 1 hora atrás

    # Definindo o endpoint da API do Watson Assistant para consulta de logs
    # url = watson_url + "/v2/assistants/" + workspace_id + "/logs"
    url = "https://escola-api.caprover.programadornoob.io/start_point/health"

    # Definindo os headers para a requisição HTTP
    # headers = {
    #     "Content-Type": "application/json",
    #     "Authorization": "Bearer " + watson_apikey,
    # }

    # Definindo os parâmetros da requisição HTTP
    # params = {"start_time": start_time, "end_time": end_time}

    # Fazendo a requisição HTTP para o Watson Assistant
    response = requests.get(url, headers=headers, params=params)

    # Verificando se a requisição foi bem sucedida
    if response.status_code != 200:
        logging.error(
            "Erro na requisição HTTP para o Watson Assistant: " + response.text
        )
        return

    # Transformando a resposta da API em um objeto JSON
    logs = json.loads(response.text)

    # Enviando os logs para o Airflow
    logging.info(
        "Enviando "
        + str(len(logs))
        + " logs do Watson Assistant para o Airflow"
    )
    for log in logs:
        # Aqui você pode fazer o que quiser com os logs do Watson Assistant
        # Neste exemplo, estamos apenas enviando para o Airflow como logs do sistema
        logging.info(log)


with DAG(
    "send_watson_logs_to_airflow",
    start_date=datetime(2023, 3, 8),
    schedule_interval=timedelta(minutes=3),
    catchup=False,
) as dag:

    send_logs_watson = PythonOperator(
        task_id="Logs Watson", python_callable=send_logs
    )

    send_logs_watson
