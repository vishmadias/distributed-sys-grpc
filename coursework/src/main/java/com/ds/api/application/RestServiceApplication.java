package com.ds.api.application;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = {"com.ds.api.controller" , "com.ds.grpc.client"})
public class RestServiceApplication extends SpringBootServletInitializer{
    public static void main(String[] args) {
        SpringApplication.run(RestServiceApplication.class, args);
    }
}
