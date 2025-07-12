package com.example.clientbenchmark;

// imports 
import com.clickhouse.client.api.Client;

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
            if(this.username != null && this.password != null){
                this.ch_client = new Client.Builder()
                .addEndpoint("http://"+ this.clickhouse_host + ":" + this.clickhouse_port + "/")
                .setUsername(this.username)
                .setPassword(this.password)
                .build();

                return true;
            }
            else{
                return false;
            }
            
        } catch (Exception e) {
            System.out.println("[ ERROR ]: Failed to connect to clickhouse due to: " + e);
            return false;
        }
    }

    public boolean is_connected(){
        try {
            return this.ch_client.ping();
        } catch (Exception e) {
            System.out.println("[ ERROR ]: Failed to ping clickhouse due to: " + e);
            return false;
        }
    }

    

    public CHConn(String username, String passsword, String clickhouse_host, int clickhouse_port) {
        set_username(username);
        set_password(passsword);
        set_clickhouse_host(passsword);
        set_clickhouse_port(clickhouse_port);
        connect();
    }
}
