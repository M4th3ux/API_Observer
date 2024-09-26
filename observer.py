import os
import logging
import requests
import psycopg2
import json
import time
from dotenv import load_dotenv
from datetime import datetime

# Carregar variáveis de ambiente
load_dotenv()

# Configurar logging
logging.basicConfig(level=logging.ERROR, filename='app.log', format='%(asctime)s %(levelname)s:%(message)s')

def fetch_data_from_endpoint(url):
    try:
        response = requests.get(url, timeout=5)  # Adicionando timeout
        response.raise_for_status()
        return response.json(), response.status_code
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao fazer a requisição para {url}: {e}")
        return None, 404  # Retorna 404 em caso de erro

def save_to_postgresql(data, url, status_code, conn_params):
    try:
        with psycopg2.connect(**conn_params) as conn:
            with conn.cursor() as cursor:
                status_code_json = json.dumps({'status_code': status_code})
                
                # Captura o timestamp atual (sem formatação)
                timestamp = datetime.now()

                insert_query = """
                INSERT INTO watcher.watcher (coluna_jsonb, rota, status_code, timestamp)
                VALUES (%s, %s, %s, %s)
                """
                cursor.execute(insert_query, [
                    json.dumps(data) if data is not None else json.dumps({'alert': 'No data available'}), 
                    url, 
                    status_code_json,
                    timestamp  # Insere o objeto datetime diretamente
                ])
                conn.commit()

                if status_code >= 400:
                    logging.warning(f"Erro inserido no banco de dados para {url}: {data} com status {status_code_json}!")
                else:
                    logging.info(f"Dados inseridos com sucesso para {url} com status {status_code_json}!")

    except Exception as e:
        logging.error(f"Erro ao inserir os dados: {e}")

if __name__ == "__main__":
    urls = os.getenv("URLS").split(',')

    conn_params = {
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT")
    }

    interval = 1

    while True:
        for url in urls:
            data, status_code = fetch_data_from_endpoint(url)
            if data is not None and status_code is not None:
                save_to_postgresql(data, url, status_code, conn_params)
            else:
                alert_message = {"alert": f"Erro ao acessar {url}."}
                save_to_postgresql(alert_message, url, 404, conn_params)

        logging.info(f"Aguardando {interval} segundos para a próxima checagem...")
        time.sleep(interval)
