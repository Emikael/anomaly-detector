package com.emikaelsilveira.anomalydetector.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/** Bootstraps the stateful anomaly-detector consumer service. */
@SpringBootApplication
@ConfigurationPropertiesScan
public class ConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ConsumerApplication.class, args);
    }
}
