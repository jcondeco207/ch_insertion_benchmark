import clickhouse_connect
import os, traceback
from dotenv import load_dotenv
import random
import uuid
from datetime import datetime, timedelta
import time


class ClickhouseConnector:
    def __init__(self):
        self.ch_client = self.open_session()
        print(f"[ CH CONNECTION ] : {self.ch_client.ping()}")
    
    def is_connected(self):
        try:
            return self.ch_client.ping()
        except Exception as e:
            print(f"[ ERROR ]: Failed to check ch connection, {e}")

    def open_session(self):
        try:
            return clickhouse_connect.get_client(
                host='localhost',
                username='demo',
                password='demo'
                )
        except Exception as e:
            print(f"[ ERROR ]: Failed to open session.")
            traceback.print_exc()
            return None

    def close_session(self):
        try:
            self.ch_client.close()
        except Exception as e:
            print(f"[ ERROR ]: Failed to close session.")

    def list_databases(self, in_df=False, in_arrow=False):
        try:
            query = "SHOW DATABASES"
            if in_df:
                return self.ch_client.query_df(query)
            elif in_arrow:
                return self.ch_client.query_arrow(query)
            else:
                return self.ch_client.query(query)
        except Exception as e:
            print(f"[ ERROR ] : Failed to list DB's")
            return None

    def create_database(self, db_name):
        try:
            self.ch_client.command(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        except Exception as e:
            print(f"[ ERROR ] : Failed to create {db_name} - {e}")
            traceback.print_exc()

    def delete_database(self, db_name):
        try:
            self.ch_client.command(f"DROP DATABASE IF EXISTS {db_name}")
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
            
            self.ch_client.command(create_table_sql)
        except Exception as e:
            print(f"[ ERROR ] : Failed to create {table_name} in {db_name} - {e}")
            traceback.print_exc()

    def delete_table(self, db_name: str, table_name:str):
        try:            
            self.ch_client.command(f"DROP TABLE IF EXISTS `{db_name}`.{table_name}")
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
            result = self.ch_client.query(query)
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
            self.ch_client.command(alter_sql)
        except Exception as e:
            print(f"[ ERROR ] : Failed to add field {field_name} to {table_name} in {db_name} - {e}")
            traceback.print_exc()

    def insert_data(self, db_name:str, table_name:str, values):
        try:
            if not values:
                return
            # Get columns from first dict
            columns = list(values[0].keys())
            # Prepare data as list of tuples
            data = [tuple(item[col] for col in columns) for item in values]
            table_full = f"`{db_name}`.{table_name}"
            self.ch_client.insert(table_full, data, column_names=columns)
        except Exception as e:
            print(f"[ ERROR ] : Failed to insert data into {table_name} in {db_name} - {e}")
            traceback.print_exc()

class Benchmark:
    def __init__(self, samples=5000, devices=10):
        self.devices = devices
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
        try:
            # generate self.samples fake data to insert into devices and uplink_data

            devices = []
            uplink_data = []

            # First, generate devices
            for i in range(self.devices):
                dev_eui = f"{random.randint(10000000, 99999999):08x}"
                device_name = f"device_{i}"
                application_id = f"app_{random.randint(1, 10)}"
                application_name = f"Application {random.randint(1, 10)}"
                device_profile_name = f"profile_{random.randint(1, 5)}"
                device_profile_id = str(uuid.uuid4())
                last_seen = datetime.now() - timedelta(minutes=random.randint(0, 10000))

                devices.append({
                    "devEui": dev_eui,
                    "device_name": device_name,
                    "application_id": application_id,
                    "application_name": application_name,
                    "device_profile_name": device_profile_name,
                    "device_profile_id": device_profile_id,
                    "last_seen": last_seen
                })

            # For each device, generate multiple uplink_data entries
            for device in devices:
                num_uplinks = int(round(self.samples / self.devices, 0))
                for _ in range(num_uplinks):
                    uplink_data.append({
                        "id": str(uuid.uuid4()),
                        "device_devEui": device["devEui"],
                        "application_id": device["application_id"],
                        "metric_id": f"metric_{random.randint(1, 20)}",
                        "metric_value": str(random.uniform(0, 100)),
                        "metric_type": random.choice(["temperature", "humidity", "pressure"]),
                        "metric_name": f"Metric {random.randint(1, 20)}",
                        "event_time": device["last_seen"] + timedelta(seconds=random.randint(0, 3600))
                    })

            return {"devices": devices, "uplink_data": uplink_data}
        except Exception as e:
            print(f"[ ERROR ]: Failed to generate sample data, {e}")
            return None

    def run(self):
        is_ready = self.health_checks()
        if not is_ready:
            print("[ ERROR ]: Health checks failed")

        sample_data = self.generate_sample_data()
        db_name = f"PyBenchmark{self.samples}"
        if sample_data:
            start_time = time.time()
            self.ch_client.insert_data(db_name, "devices", sample_data["devices"])
            self.ch_client.insert_data(db_name, "uplink_data", sample_data["uplink_data"])
            end_time = time.time()
            elapsed = end_time - start_time
            print(f"[ INFO ]: Data insertion took {elapsed:.4f} seconds")

bm = Benchmark()
for i in range(3):
    bm.run()