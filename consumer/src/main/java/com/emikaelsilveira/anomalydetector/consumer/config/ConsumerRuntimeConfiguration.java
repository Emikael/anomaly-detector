package com.emikaelsilveira.anomalydetector.consumer.config;

import java.time.Clock;

import com.emikaelsilveira.anomalydetector.consumer.detection.ZScoreDetector;
import com.emikaelsilveira.anomalydetector.consumer.logging.DatapointEventLogger;
import com.emikaelsilveira.anomalydetector.consumer.logging.DatapointLogFormatter;
import com.emikaelsilveira.anomalydetector.consumer.metrics.ConsumerMetrics;
import com.emikaelsilveira.anomalydetector.consumer.processing.BoundedIdCache;
import com.emikaelsilveira.anomalydetector.consumer.processing.DatapointProcessor;
import com.emikaelsilveira.anomalydetector.consumer.processing.EventTimeMonitor;
import com.emikaelsilveira.anomalydetector.consumer.processing.SequenceTracker;
import com.emikaelsilveira.anomalydetector.consumer.validation.DatapointValidator;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class ConsumerRuntimeConfiguration {

    @Bean
    Clock processingClock() {
        return Clock.systemUTC();
    }

    @Bean
    ZScoreDetector zScoreDetector(DetectorProperties properties) {
        return new ZScoreDetector(
                properties.windowSize(),
                properties.minSamples(),
                properties.zThreshold(),
                properties.excludeAnomalies(),
                properties.consecutiveOverride()
        );
    }

    @Bean
    BoundedIdCache boundedIdCache(DetectorProperties properties) {
        return new BoundedIdCache(10 * properties.windowSize());
    }

    @Bean
    SequenceTracker sequenceTracker() {
        return new SequenceTracker();
    }

    @Bean
    EventTimeMonitor eventTimeMonitor(Clock processingClock) {
        return new EventTimeMonitor(processingClock);
    }

    @Bean
    DatapointValidator datapointValidator() {
        return new DatapointValidator();
    }

    @Bean
    DatapointLogFormatter datapointLogFormatter() {
        return new DatapointLogFormatter();
    }

    @Bean
    DatapointEventLogger datapointEventLogger(DatapointLogFormatter formatter) {
        return new DatapointEventLogger(LoggerFactory.getLogger(DatapointEventLogger.class), formatter);
    }

    @Bean
    ConsumerMetrics consumerMetrics(MeterRegistry meterRegistry, Clock processingClock, DetectorProperties properties) {
        return new ConsumerMetrics(meterRegistry, processingClock, properties.summaryEvery(), properties.windowSize());
    }

    @Bean
    DatapointProcessor datapointProcessor(
            DatapointValidator validator,
            BoundedIdCache idCache,
            SequenceTracker sequenceTracker,
            EventTimeMonitor eventTimeMonitor,
            ZScoreDetector detector,
            DatapointEventLogger eventLogger,
            ConsumerMetrics metrics
    ) {
        return new DatapointProcessor(
                validator,
                idCache,
                sequenceTracker,
                eventTimeMonitor,
                detector,
                eventLogger,
                metrics
        );
    }
}
