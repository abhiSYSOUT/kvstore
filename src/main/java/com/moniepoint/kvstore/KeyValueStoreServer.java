package com.moniepoint.kvstore;

import com.moniepoint.kvstore.engine.KeyValueStoreEngine;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;

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
            switch (inputs[0]) {
                case "PUT" -> {
                    keyValueStoreEngine.put(inputs[1], inputs[2]);
                    output.write("OK \n");
                }
                case "GET" -> {
                    String value = keyValueStoreEngine.read(inputs[1]);
                    output.write((value == null ? "NULL" : value) + " \n");
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
}
