package com.ds.grpc.server;

import ch.qos.logback.classic.Level;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;

public class GrpcServer extends Thread {
    private int port;
    private int threadNumber;
    private static int[] ports = {8081, 8082, 8083, 8084, 8085, 8086, 8087, 8088};

    public GrpcServer(int port, int threadNumber) {
        this.port = port;
        this.threadNumber = threadNumber;
    }

    public static void main(String[] args) {
        ((ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory
                .getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME))
                .setLevel(Level.INFO);

        // get the number of processors available
        Runtime runtime = Runtime.getRuntime();
        int numberOfProcessors = runtime.availableProcessors();
        System.out.println("Number of processors available : " + numberOfProcessors);

        for (int i = 0; i < numberOfProcessors; i++) {
            new GrpcServer(ports[i], i + 1).start();
        }
    }

    public void run() {
        Server server = ServerBuilder.forPort(port)
                .addService(new MatrixServiceGrpcImpl(threadNumber)).build();


        System.out.println("Starting server " + threadNumber + " on " + port);

        try {
            server.start();
            System.out.println("Server " + threadNumber + " started on " + port);
            System.out.println("=============================");
            server.awaitTermination();

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
