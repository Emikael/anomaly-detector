package com.emikaelsilveira.anomalydetector.producer.production;

import java.lang.reflect.Method;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.emikaelsilveira.anomalydetector.producer.contract.Datapoint;
import com.emikaelsilveira.anomalydetector.producer.generation.DatapointGenerator;
import com.emikaelsilveira.anomalydetector.producer.generation.GeneratedDatapoint;
import com.emikaelsilveira.anomalydetector.producer.logging.ProducerEventLogger;
import com.emikaelsilveira.anomalydetector.producer.messaging.DatapointPublisher;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.scheduling.annotation.Scheduled;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class DatapointProductionServiceTest {

    @Test
    void generatesPublishesAndLogsExactlyOncePerTick() {
        DatapointGenerator generator = mock(DatapointGenerator.class);
        DatapointPublisher publisher = mock(DatapointPublisher.class);
        ProducerEventLogger eventLogger = mock(ProducerEventLogger.class);
        GeneratedDatapoint generated = generated();
        when(generator.next()).thenReturn(generated);
        DatapointProductionService service = new DatapointProductionService(
                generator, publisher, eventLogger, org.slf4j.LoggerFactory.getLogger("producer-service-test"));

        service.produce();

        verify(generator).next();
        verify(publisher).publish(generated.datapoint());
        verify(eventLogger).log(generated);
    }


    @Test
    void logsAndContainsATransientPublishFailureWithoutStoppingFutureTicks() {
        DatapointGenerator generator = mock(DatapointGenerator.class);
        DatapointPublisher publisher = mock(DatapointPublisher.class);
        ProducerEventLogger eventLogger = mock(ProducerEventLogger.class);
        GeneratedDatapoint generated = generated();
        when(generator.next()).thenReturn(generated);
        doThrow(new AmqpConnectException("broker unavailable", new IllegalStateException())).when(publisher)
                .publish(generated.datapoint());
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger("producer-service-failure-test");
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            DatapointProductionService service = new DatapointProductionService(generator, publisher, eventLogger, logger);

            assertThatCode(() -> {
                service.produce();
                service.produce();
            }).doesNotThrowAnyException();

            verify(publisher, org.mockito.Mockito.times(2)).publish(generated.datapoint());
            verifyNoInteractions(eventLogger);
            assertThat(appender.list).hasSize(2).allSatisfy(event -> {
                assertThat(event.getLevel()).isEqualTo(Level.ERROR);
                assertThat(event.getFormattedMessage()).contains("Unable to publish generated datapoint");
            });
        } finally {
            logger.detachAppender(appender);
            appender.stop();
        }
    }


    @Test
    void schedulesWithTheConfiguredFixedRate() throws Exception {
        Method produce = DatapointProductionService.class.getDeclaredMethod("produce");

        assertThat(produce.getAnnotation(Scheduled.class).fixedRateString()).isEqualTo("${producer.interval-ms}");
    }

    private GeneratedDatapoint generated() {
        return new GeneratedDatapoint(
                new Datapoint(
                        UUID.fromString("0f3a9c1e-6b7d-4a2f-9c11-8de4b5a70c93"),
                        1041,
                        "sensor.temperature",
                        152.88,
                        Instant.parse("2026-08-05T14:22:07.361Z")
                ),
                Optional.empty(),
                Optional.empty()
        );
    }
}
