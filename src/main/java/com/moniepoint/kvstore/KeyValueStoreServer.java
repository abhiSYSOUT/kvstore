package com.moniepoint.kvstore;

import com.moniepoint.kvstore.engine.KeyValueStoreEngine;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.Map;

public class KeyValueStoreServer {

    public static void main(String[] args) throws Exception {
        KeyValueStoreEngine keyValueStoreEngine = new KeyValueStoreEngine(Path.of("data"), (long) (64 * 1024 * 1024));
        ServerSocket serverSocket = new ServerSocket(9191);

        while (true) {
            Socket socket = serverSocket.accept();
            new Thread( () -> handle(socket, keyValueStoreEngine)).start();
        }
    }

    private static void handle(Socket socket, KeyValueStoreEngine keyValueStoreEngine) {
        try (
            BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            BufferedWriter output = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
        ) {
            String line = input.readLine();

            if (line == null) {
                return;
            }

            String[] inputs = line.split(" ", 3);

            if (line.startsWith("BATCH")) {
                handleBatch(line, input, output, keyValueStoreEngine);
                return;
            }

            switch (inputs[0]) {
                case "PUT" -> {
                    keyValueStoreEngine.put(inputs[1], inputs[2]);
                    output.write("OK \n");
                }
                case "GET" -> {
                    if (inputs.length == 2) {
                        String value = keyValueStoreEngine.read(inputs[1]);
                        output.write((value == null ? "NULL" : value) + " \n");
                    } else if (inputs.length == 3) {
                        Map<String, String> rangeValues = keyValueStoreEngine.readKeyRange(inputs[1], inputs[2]);
                        for (String key : rangeValues.keySet()) {
                            output.write((rangeValues.get(key) == null ? "NULL" : rangeValues.get(key)) + " \n");
                        }
                    }
                }
                case "DEL" -> {
                    keyValueStoreEngine.delete(inputs[1]);
                    output.write("OK \n");
                }
                default -> {
                    output.write("Unsupported command " + inputs[0] + " \n");
                }
            }
            output.flush();
        } catch (Exception e) {
            System.err.println("Ignoring exception for now " + e.getMessage());
        }
    }

    private static void handleBatch(String header, BufferedReader input, BufferedWriter output, KeyValueStoreEngine keyValueStoreEngine) throws Exception {
        String[] headerData = header.split(" ", 2);
        int totalItems = Integer.parseInt(headerData[1]);

        for (int i = 0; i < totalItems; i++) {
            String line = input.readLine();

            if (line == null) {
                throw new IOException("Invalid input");
            }

            String[] keyValue = line.split(" ", 2);
            keyValueStoreEngine.put(keyValue[0], keyValue[1]);
        }

        output.write("OK \n");
    }
}
