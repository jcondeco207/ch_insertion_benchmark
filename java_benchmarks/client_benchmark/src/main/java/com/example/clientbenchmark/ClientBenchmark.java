package com.example.clientbenchmark;

import java.util.ArrayList;
import java.util.List;

public class ClientBenchmark {
    public static void main(String[] args) {
        String db_name = "Test1";

        CHConn ch_connector = new CHConn(
                "demo",
                "demo",
                "localhost",
                8123);

        boolean is_connected = ch_connector.is_connected();
        System.out.println("Is connected: " + is_connected);

        if (is_connected) {
            // boolean db_created = ch_connector.create_database(db_name);
            List<String> dbs = ch_connector.list_databases();
            System.out.println("Databases: " + dbs);


            // if(db_created){
                List<String> uplink_fields = new ArrayList<>();
                uplink_fields.add("id UUID DEFAULT generateUUIDv4()");
                uplink_fields.add("device_devEui String");
                uplink_fields.add("application_id String");
                uplink_fields.add("metric_id String");
                uplink_fields.add("metric_value String");
                uplink_fields.add("metric_type String");
                uplink_fields.add("metric_name String");
                uplink_fields.add("event_time DateTime64");

                boolean table_created = ch_connector.create_table(
                    db_name, 
                    "uplink_events", 
                    uplink_fields, 
                    "event_time", 
                    "id", 
                    "ReplacingMergeTree()");

                boolean table_exists = ch_connector.check_if_table_exists(db_name, "uplink_events");

                System.out.println("Table exists: " + (table_created && table_exists));

                System.out.println(ch_connector.show_database_tables(db_name));

                // boolean db_deleted = ch_connector.delete_database(db_name);
            // }
            System.out.println("Is disconnected: " + ch_connector.disconnect());
        }
    }
}
