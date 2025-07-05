import clickhouse_connect
import os, traceback
from dotenv import load_dotenv


class ClickhouseConnector:
    def __init__(self):
        self.open_session()
        print(f"[ CH CONNECTION ] : {self.client.ping()}")
    
    def is_connected(self):
        try:
            return self.client.ping()
        except Exception as e:
            print(f"[ ERROR ]: Failed to check ch connection, {e}")

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


class Benchmark:
    def __init__(self, samples=5000):
        self.samples = samples
        self.ch_client = ClickhouseConnector()
    
    def health_checks(self) -> bool:
        tests = [
            self.ch_client.is_connected, 
            self.check_database,
            # check table
        ]
        pass

    def check_database(self) -> bool:
        try:
            return True
        except Exception as e:
            print(f"[ ERROR ]: failed at check_database, {e}")
            return False
    

# test = ClickhouseConnector()
# test.close_session()