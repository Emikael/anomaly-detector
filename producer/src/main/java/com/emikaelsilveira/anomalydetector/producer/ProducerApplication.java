package com.emikaelsilveira.anomalydetector.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;


/** Bootstraps the producer service and enables scheduled datapoint generation. */
@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
public class ProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProducerApplication.class, args);
    }
}
