import clickhouse_connect
import os, traceback
from dotenv import load_dotenv


class ClickhouseConnector:
    def __init__(self):
        self.open_session()
        print(f"[ CH CONNECTION ] : {self.client.ping()}")

    def open_session(self):
        try:
            load_dotenv(dotenv_path='.env_local')
            self.client = clickhouse_connect.get_client(
                host=os.getenv('CLICKHOUSE_HOST', 'localhost'),
                username=os.getenv('CLICKHOUSE_USER', 'demo'),
                password=os.getenv('CLICKHOUSE_PASSWORD', 'demo')
            )
        except Exception as e:
            print(f"[ ERROR ]: Failed to open session.")

    def close_session(self):
        try:
            self.client.close()
        except Exception as e:
            print(f"[ ERROR ]: Failed to close session.")

    def list_databases(self):
        try:
            databases = self.client.query_df(f"SHOW DATABASES")
            return databases
        except Exception as e:
            print(f"[ ERROR ] : Failed to list DB's")
            return None

    def create_database():
        pass

    def delete_database():
        pass

test = ClickhouseConnector()
test.close_session()