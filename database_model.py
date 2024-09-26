import psycopg2
import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# Parâmetros de conexão do PostgreSQL
conn_params = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

def create_tables():
    try:
        # Conectar ao banco de dados PostgreSQL
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()

        # Comando SQL para criar a tabela
        create_table_query = """
        CREATE SCHEMA IF NOT EXISTS watcher;

        CREATE TABLE IF NOT EXISTS watcher.watcher (
            id SERIAL PRIMARY KEY,
            coluna_jsonb JSONB,
            rota TEXT NOT NULL,
            status_code JSONB,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        # Executar o comando de criação da tabela
        cursor.execute(create_table_query)
        conn.commit()

        print("Tabela criada com sucesso.")

    except Exception as e:
        print(f"Erro ao criar a tabela: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    create_tables()
