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

    def list_databases(self, in_df=False, in_arrow=False):
        try:
            query = "SHOW DATABASES"
            if in_df:
                return self.client.query_df(query)
            elif in_arrow:
                return self.client.query_arrow(query)
            else:
                return self.client.query(query)
        except Exception as e:
            print(f"[ ERROR ] : Failed to list DB's")
            return None

    def create_database(self, db_name):
        try:
            self.client.command(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        except Exception as e:
            print(f"[ ERROR ] : Failed to create {db_name} - {e}")
            traceback.print_exc()

    def delete_database(self, db_name):
        try:
            self.client.command(f"DROP DATABASE IF EXISTS {db_name}")
        except Exception as e:
            print(f"[ ERROR ] : Failed to delete {db_name} - {e}")
            traceback.print_exc()

    def create_table(
            self, 
            db_name: str, 
            table_name:str, 
            fields: list[str],
            time_variable_name: str,
            engine: str = "ReplacingMergeTree()"):
        try:

            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS `{db_name}`.{table_name} (
                {',\n'.join(fields)}
                )
                ENGINE = {engine}
                PARTITION BY toMonday({time_variable_name})
                ORDER BY ({time_variable_name})
                """
            
            self.client.command(create_table_sql)
        except Exception as e:
            print(f"[ ERROR ] : Failed to create {table_name} in {db_name} - {e}")
            traceback.print_exc()

    def delete_table(self, db_name: str, table_name:str):
        try:            
            self.client.command(f"DROP TABLE IF EXISTS `{db_name}`.{table_name}")
        except Exception as e:
            print(f"[ ERROR ] : Failed to drop {table_name} in {db_name} - {e}")
            traceback.print_exc()

    def check_if_table_exists(self, db_name: str, table_name:str):
        try:
            query = f"""
                SELECT count()
                FROM system.tables
                WHERE database = '{db_name}' AND name = '{table_name}'
            """
            result = self.client.query(query)
            return result.result_rows[0][0] > 0
        except Exception as e:
            print(f"[ ERROR ] : Failed to check if table exists - {e}")
            traceback.print_exc()
            return False

    def add_field_to_table(self, db_name:str, table_name:str, field_name:str, field_type:str, default_value):
        try:
            alter_sql = f"""
                ALTER TABLE `{db_name}`.{table_name}
                ADD COLUMN IF NOT EXISTS {field_name} {field_type} DEFAULT {repr(default_value)}
            """
            self.client.command(alter_sql)
        except Exception as e:
            print(f"[ ERROR ] : Failed to add field {field_name} to {table_name} in {db_name} - {e}")
            traceback.print_exc()

    def insert_data(self, db_name:str, table_name:str, values):
        pass

class Benchmark:
    def __init__(self, samples=5000):
        self.samples = samples
        self.ch_client = ClickhouseConnector()
    
    def health_checks(self) -> bool:
        tests = [
            self.ch_client.is_connected, 
            self.check_database,
            self.check_tables
        ]

        for test in tests:
            check = test()
            if not check:
                return False
        return True

    def check_database(self) -> bool:
        """
        - Drop Database f"PyBenchmark{self.samples}"
        - Create Database with name f"PyBenchmark{self.samples}"
        """
        try:
            db_name = f"PyBenchmark{self.samples}"
            self.ch_client.delete_database(db_name)
            self.ch_client.create_database(db_name)
            return True
        except Exception as e:
            print(f"[ ERROR ]: failed at check_database, {e}")
            return False
        
    def check_tables(self) -> bool:
        """
        Create table "Devices" with fields: 
            - devEui String 
            - device_name String
            - application_id String
            - application_name String
            - device_profile_name String
            - device_profile_id String
            - last_seen DateTime64

        Create table "uplink_data" with fields: 
            - id UUID DEFAULT generateUUIDv4() 
            - device_devEui String
            - application_id String
            - metric_id String
            - metric_value String
            - metric_type String
            - metric_name String
            - event_time DateTime64
        """
        try:
            devices_fields = [
                "devEui String",
                "device_name String",
                "application_id String",
                "application_name String",
                "device_profile_name String",
                "device_profile_id String",
                "last_seen DateTime64"
            ]
            uplink_fields = [
                "id UUID DEFAULT generateUUIDv4()",
                "device_devEui String",
                "application_id String",
                "metric_id String",
                "metric_value String",
                "metric_type String",
                "metric_name String",
                "event_time DateTime64"
            ]
            db_name = f"PyBenchmark{self.samples}"
            self.ch_client.delete_table(db_name, "devices")
            self.ch_client.delete_table(db_name, "uplink_data")
            self.ch_client.create_table(db_name, "devices", devices_fields, "last_seen")
            self.ch_client.create_table(db_name, "uplink_data", uplink_fields, "event_time")
            return True
        except Exception as e:
            print(f"[ ERROR ]: failed at check_database, {e}")
            return False

    def generate_sample_data(self):
        pass

    def run(self):
        is_ready = self.health_checks()
        if not is_ready:
            print("[ ERROR ]: Health checks failed")

        self.generate_sample_data()