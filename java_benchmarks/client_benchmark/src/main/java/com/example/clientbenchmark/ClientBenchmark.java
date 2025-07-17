package com.example.clientbenchmark;

import java.util.List;

public class ClientBenchmark {
    public static void main(String[] args) {
        CHConn ch_connector = new CHConn(
                "demo",
                "demo",
                "localhost",
                8123);

        System.out.println("Is connected: " + ch_connector.is_connected());

        List<String> dbs = ch_connector.getDatabaseNames();
        System.out.println(dbs);

        System.out.println("Is disconnected: " + ch_connector.disconnect());

    }
}
