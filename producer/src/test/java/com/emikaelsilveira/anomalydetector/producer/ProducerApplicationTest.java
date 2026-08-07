package com.emikaelsilveira.anomalydetector.producer;

import com.emikaelsilveira.anomalydetector.producer.production.DatapointProductionService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;

@SpringBootTest(
        classes = ProducerApplication.class,
        properties = "producer.scheduling-enabled=false",
        webEnvironment = SpringBootTest.WebEnvironment.NONE
)
class ProducerApplicationTest {
    @Autowired
    private ApplicationContext applicationContext;

    @Test
    void contextLoadsWithoutStartingProductionWhenSchedulingIsDisabled() {
        org.assertj.core.api.Assertions.assertThat(
                applicationContext.getBeansOfType(DatapointProductionService.class)
        ).isEmpty();
    }
}
