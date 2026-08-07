package com.emikaelsilveira.anomalydetector.consumer.config;

import com.emikaelsilveira.anomalydetector.consumer.ConsumerApplication;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        classes = ConsumerApplication.class,
        properties = "spring.rabbitmq.listener.simple.auto-startup=false",
        webEnvironment = SpringBootTest.WebEnvironment.NONE
)
class DetectorPropertiesDefaultsTest {

    @Autowired
    private DetectorProperties detectorProperties;

    @Test
    void defaultsBindFromApplicationConfiguration() {
        assertThat(detectorProperties).isEqualTo(new DetectorProperties(50, 3.0, 30, true, 5, 100));
    }
}
