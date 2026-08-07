package com.emikaelsilveira.anomalydetector.consumer.messaging;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import com.emikaelsilveira.anomalydetector.consumer.contract.Datapoint;
import com.emikaelsilveira.anomalydetector.consumer.metrics.ConsumerMetrics;
import com.emikaelsilveira.anomalydetector.consumer.processing.DatapointProcessor;
import com.emikaelsilveira.anomalydetector.consumer.processing.ProcessingOutcome;
import com.emikaelsilveira.anomalydetector.consumer.validation.InvalidDatapointException;
import com.rabbitmq.client.Channel;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DatapointListenerTest {

    private SimpleMeterRegistry registry;
    private DatapointProcessor processor;
    private Channel channel;
    private DatapointListener listener;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        processor = mock(DatapointProcessor.class);
        channel = mock(Channel.class);
        listener = new DatapointListener(
                processor,
                new ConsumerMetrics(
                        registry,
                        Clock.fixed(Instant.parse("2026-08-05T14:22:07.361Z"), ZoneOffset.UTC),
                        100,
                        50
                )
        );
    }

    @AfterEach
    void tearDown() {
        registry.close();
    }

    @Test
    void acknowledgesOnlyAfterTheProcessorReturnsSuccessfully() throws Exception {
        Datapoint datapoint = datapoint();
        org.springframework.amqp.core.Message rawMessage = message(41L);
        when(processor.process(datapoint)).thenReturn(ProcessingOutcome.PROCESSED);

        listener.receive(datapoint, rawMessage, channel);

        verify(processor).process(datapoint);
        verify(channel).basicAck(41L, false);
    }

    @Test
    void rejectsInvalidDatapointsWithoutRequeueAndIncrementsRejectedMetric() throws Exception {
        Datapoint datapoint = datapoint();
        org.springframework.amqp.core.Message rawMessage = message(42L);
        doThrow(new InvalidDatapointException("metric")).when(processor).process(datapoint);

        listener.receive(datapoint, rawMessage, channel);

        verify(channel).basicReject(42L, false);
        assertThat(registry.get("anomaly.detector.points.rejected").counter().count()).isEqualTo(1.0d);
    }

    @Test
    void letsUnexpectedProcessingFailuresReachTheRetryAdviceWithoutTerminalAck() throws Exception {
        Datapoint datapoint = datapoint();
        org.springframework.amqp.core.Message rawMessage = message(43L);
        IllegalStateException failure = new IllegalStateException("database unavailable");
        doThrow(failure).when(processor).process(datapoint);

        assertThatThrownBy(() -> listener.receive(datapoint, rawMessage, channel))
                .isSameAs(failure);

        verify(processor).process(datapoint);
        verify(channel, org.mockito.Mockito.never()).basicAck(
                org.mockito.ArgumentMatchers.anyLong(),
                org.mockito.ArgumentMatchers.anyBoolean()
        );
        verify(channel, org.mockito.Mockito.never()).basicReject(org.mockito.ArgumentMatchers.anyLong(), org.mockito.ArgumentMatchers.anyBoolean());
    }

    private Datapoint datapoint() {
        return new Datapoint(
                UUID.fromString("0f3a9c1e-6b7d-4a2f-9c11-8de4b5a70c93"),
                1L,
                "sensor.temperature",
                1.0d,
                Instant.parse("2026-08-05T14:22:07.361Z")
        );
    }

    private org.springframework.amqp.core.Message message(long deliveryTag) {
        org.springframework.amqp.core.MessageProperties properties = new org.springframework.amqp.core.MessageProperties();
        properties.setDeliveryTag(deliveryTag);
        return new org.springframework.amqp.core.Message("{}".getBytes(StandardCharsets.UTF_8), properties);
    }
}
