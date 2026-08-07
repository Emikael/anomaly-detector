package com.emikaelsilveira.anomalydetector.consumer;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(
        classes = ConsumerApplication.class,
        properties = "spring.rabbitmq.listener.simple.auto-startup=false",
        webEnvironment = SpringBootTest.WebEnvironment.NONE
)
class ConsumerApplicationTest {

    @Test
    void contextLoads() {
    }
}
