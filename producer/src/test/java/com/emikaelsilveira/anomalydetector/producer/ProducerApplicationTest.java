package com.emikaelsilveira.anomalydetector.producer;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(
        classes = ProducerApplication.class,
        properties = "spring.task.scheduling.enabled=false",
        webEnvironment = SpringBootTest.WebEnvironment.NONE
)
class ProducerApplicationTest {

    @Test
    void contextLoads() {
    }
}
