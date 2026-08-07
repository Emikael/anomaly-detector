package com.emikaelsilveira.anomalydetector.consumer.config;

import com.emikaelsilveira.anomalydetector.consumer.ConsumerApplication;
import com.emikaelsilveira.anomalydetector.consumer.detection.ZScoreDetector;
import com.emikaelsilveira.anomalydetector.consumer.metrics.ConsumerMetrics;
import com.emikaelsilveira.anomalydetector.consumer.messaging.DatapointListener;
import com.emikaelsilveira.anomalydetector.consumer.processing.BoundedIdCache;
import com.emikaelsilveira.anomalydetector.consumer.processing.DatapointProcessor;
import com.emikaelsilveira.anomalydetector.consumer.processing.EventTimeMonitor;
import com.emikaelsilveira.anomalydetector.consumer.processing.SequenceTracker;
import com.emikaelsilveira.anomalydetector.consumer.validation.DatapointValidator;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        classes = ConsumerApplication.class,
        properties = {
                "spring.rabbitmq.listener.simple.auto-startup=false",
                "detector.window-size=60",
                "detector.z-threshold=4.5",
                "detector.min-samples=35",
                "detector.exclude-anomalies=false",
                "detector.consecutive-override=6",
                "detector.summary-every=7"
        },
        webEnvironment = SpringBootTest.WebEnvironment.NONE
)
class ConsumerRuntimeConfigurationTest {

    @Autowired
    private DetectorProperties detectorProperties;

    @Autowired
    private DatapointProcessor processor;

    @Autowired
    private ZScoreDetector detector;

    @Autowired
    private BoundedIdCache idCache;

    @Autowired
    private SequenceTracker sequenceTracker;

    @Autowired
    private EventTimeMonitor eventTimeMonitor;

    @Autowired
    private DatapointValidator datapointValidator;

    @Autowired
    private ConsumerMetrics consumerMetrics;
    @Autowired
    private DatapointListener datapointListener;


    @Test
    void wiresConfiguredDetectorCollaboratorsIntoTheProcessorRuntime() {
        assertThat(detectorProperties).isEqualTo(new DetectorProperties(60, 4.5d, 35, false, 6, 7));
        assertThat(processor).isNotNull();
        assertThat(detector).isNotNull();
        assertThat(idCache).isNotNull();
        assertThat(sequenceTracker).isNotNull();
        assertThat(eventTimeMonitor).isNotNull();
        assertThat(datapointValidator).isNotNull();
        assertThat(consumerMetrics).isNotNull();
        assertThat(datapointListener).isNotNull();
    }
}
