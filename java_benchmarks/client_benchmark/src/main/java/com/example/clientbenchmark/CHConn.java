package com.example.clientbenchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseProtocol;
// imports 
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.api.query.Records;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.data.ClickHouseFormat;

public class CHConn {
    private Client ch_client;
    private String username;
    private String password;
    private String clickhouse_host;
    private int clickhouse_port;

    private void set_username(String username) {
        this.username = username;
    }

    private void set_password(String password) {
        this.password = password;
    }

    private void set_clickhouse_host(String clickhouse_host) {
        this.clickhouse_host = clickhouse_host;
    }

    private void set_clickhouse_port(int clickhouse_port) {
        this.clickhouse_port = clickhouse_port;
    }

    public boolean connect() {
        try {
            if (this.username != null && this.password != null) {
                this.ch_client = new Client.Builder()
                        .addEndpoint("http://" + this.clickhouse_host + ":" + this.clickhouse_port + "/")
                        .setUsername(this.username)
                        .setPassword(this.password)
                        // allow experimental JSON type
                        .serverSetting("allow_experimental_json_type", "1")
                        // allow JSON transcoding as a string
                        .serverSetting(ServerSettings.INPUT_FORMAT_BINARY_READ_JSON_AS_STRING, "1")
                        .serverSetting(ServerSettings.OUTPUT_FORMAT_BINARY_WRITE_JSON_AS_STRING, "1")
                        .build();

                return true;
            } else {
                System.out.println("[ ERROR ]: Failed to connect to clickhouse due to: "
                        +
                        "username/password/host/port not being provided");
                return false;
            }

        } catch (Exception e) {
            System.out.println("[ ERROR ]: Failed to connect to clickhouse due to: " + e);
            return false;
        }
    }

    public boolean disconnect() {
        try {
            this.ch_client.close();
            return !this.is_connected();
        } catch (Exception e) {
            System.out.println("[ ERROR ]: Failed to disconnect clickhouse due to: " + e);
            return false;
        }
    }

    public boolean is_connected() {
        try {
            return this.ch_client.ping();
        } catch (Exception e) {
            System.out.println("[ ERROR ]: Failed to ping clickhouse due to: " + e);
            return false;
        }
    }

    public void readData() {
    }

    public void getDatabaseNames() {
        System.out.println("Reading data from table: {} using Records iterator");
        final String sql = "SHOW DATABASES";
        try (Records records = this.ch_client.queryRecords(sql).get(3, TimeUnit.SECONDS);) {

            System.out.println(records.getResultRows());
            // Iterate thru records
            for (GenericRecord record : records) {
                System.out.println(record);
            }
        } catch (Exception e) {
            // ERROR
        }
    }

    public CHConn(String username, String passsword, String clickhouse_host, int clickhouse_port) {
        set_username(username);
        set_password(passsword);
        set_clickhouse_host(clickhouse_host);
        set_clickhouse_port(clickhouse_port);
        connect();
    }
}
