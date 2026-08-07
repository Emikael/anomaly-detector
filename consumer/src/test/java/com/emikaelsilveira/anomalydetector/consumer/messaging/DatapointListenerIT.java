package com.emikaelsilveira.anomalydetector.consumer.messaging;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.emikaelsilveira.anomalydetector.consumer.ConsumerApplication;
import com.emikaelsilveira.anomalydetector.consumer.contract.Datapoint;
import com.emikaelsilveira.anomalydetector.consumer.processing.DatapointProcessor;
import com.emikaelsilveira.anomalydetector.consumer.processing.ProcessingOutcome;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@SpringBootTest(
        classes = ConsumerApplication.class,
        properties = "spring.rabbitmq.listener.simple.auto-startup=false",
        webEnvironment = SpringBootTest.WebEnvironment.NONE
)
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class DatapointListenerIT {

    private static final long WAIT_SECONDS = 20L;
    private static final Instant EMITTED_AT = Instant.parse("2026-08-05T14:22:07.361Z");

    @Container
    private static final RabbitMQContainer RABBIT = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:3.13-management-alpine")
    );

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private MeterRegistry meterRegistry;

    @Autowired
    private RabbitListenerEndpointRegistry listenerRegistry;

    @MockitoSpyBean
    private DatapointProcessor processor;

    private Logger processorLogger;
    private ListAppender<ILoggingEvent> processorAppender;
    private Logger listenerLogger;
    private ListAppender<ILoggingEvent> listenerAppender;

    @DynamicPropertySource
    static void rabbitProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.rabbitmq.host", RABBIT::getHost);
        registry.add("spring.rabbitmq.port", RABBIT::getAmqpPort);
        registry.add("spring.rabbitmq.username", RABBIT::getAdminUsername);
        registry.add("spring.rabbitmq.password", RABBIT::getAdminPassword);
    }

    @BeforeEach
    void clearQueuesAndCaptureWarnings() {
        stopListeners();
        rabbitTemplate.execute(channel -> {
            channel.queuePurge(RabbitTopology.DATAPOINT_QUEUE);
            channel.queuePurge(RabbitTopology.DATAPOINT_DLQ);
            return null;
        });
        processorLogger = (Logger) org.slf4j.LoggerFactory.getLogger(DatapointProcessor.class);
        processorAppender = new ListAppender<>();
        processorAppender.start();
        processorLogger.addAppender(processorAppender);
        // The rejection is logged by the listener, which owns the ack decision, not by the processor.
        listenerLogger = (Logger) org.slf4j.LoggerFactory.getLogger(DatapointListener.class);
        listenerAppender = new ListAppender<>();
        listenerAppender.start();
        listenerLogger.addAppender(listenerAppender);
        listenerRegistry.start();
    }

    @AfterEach
    void stopCapturingWarnings() {
        stopListeners();
        processorLogger.detachAppender(processorAppender);
        processorAppender.stop();
        listenerLogger.detachAppender(listenerAppender);
        listenerAppender.stop();
    }


    @Test
    void acknowledgesAHappyDeliveryAfterItIsProcessed() {
        send(datapoint(1L, 0.0d));

        await(() -> processedCount() == 1.0d);

        assertThat(queueDepth(RabbitTopology.DATAPOINT_QUEUE)).isZero();
        assertThat(queueDepth(RabbitTopology.DATAPOINT_DLQ)).isZero();
    }


    @Test
    void deadLettersMalformedJsonExactlyOnce() {
        sendRaw("{not-json");

        Message deadLetter = receiveEventually(RabbitTopology.DATAPOINT_DLQ);

        assertThat(deadLetter).isNotNull();
        assertThat(rabbitTemplate.receive(RabbitTopology.DATAPOINT_DLQ, 1_000)).isNull();
        assertThat(rejectedCount()).isEqualTo(1.0d);
    }


    @Test
    void deadLettersMissingRequiredCreatorValuesExactlyOnce() {
        sendRaw("""
                {"id":"0f3a9c1e-6b7d-4a2f-9c11-8de4b5a70c93","metric":"sensor.temperature","value":1.0,"emittedAt":"2026-08-05T14:22:07.361Z"}
                """);

        Message deadLetter = receiveEventually(RabbitTopology.DATAPOINT_DLQ);

        assertThat(deadLetter).isNotNull();
        assertThat(rabbitTemplate.receive(RabbitTopology.DATAPOINT_DLQ, 1_000)).isNull();
        assertThat(rejectedCount()).isEqualTo(1.0d);
    }


    @Test
    void deadLettersNanInfinityAndOutOfRangeValues() {
        sendRaw(datapointJson(UUID.randomUUID(), 1L, "NaN"));
        sendRaw(datapointJson(UUID.randomUUID(), 2L, "Infinity"));
        sendRaw(datapointJson(UUID.randomUUID(), 3L, "1e151"));

        assertThat(receiveEventually(RabbitTopology.DATAPOINT_DLQ)).isNotNull();
        assertThat(receiveEventually(RabbitTopology.DATAPOINT_DLQ)).isNotNull();
        assertThat(receiveEventually(RabbitTopology.DATAPOINT_DLQ)).isNotNull();
        assertThat(rejectedCount()).isEqualTo(3.0d);
        // Only the 1e151 send is guaranteed to reach the validator; NaN and Infinity may be
        // rejected earlier as conversion failures, which increment the same counter. Dead-lettering
        // is terminal, so the offending field has to be named in the log, not just counted.
        await(() -> listenerMessages().stream().anyMatch(message ->
                message.contains("Dead-lettering invalid datapoint")
                        && message.contains("field=value")
                        && message.contains("sequence=3")));
    }


    @Test
    void acknowledgesDuplicateDeliveriesWithoutSecondDetection() {
        Datapoint datapoint = datapoint(1L, 0.0d);
        send(datapoint);
        send(datapoint);

        await(() -> processedCount() == 1.0d && duplicateCount() == 1.0d);

        verify(processor, timeout(TimeUnit.SECONDS.toMillis(WAIT_SECONDS)).times(2)).process(datapoint);
        assertThat(queueDepth(RabbitTopology.DATAPOINT_QUEUE)).isZero();
    }


    @Test
    void processesGapAndOutOfOrderSequencesInArrivalOrder() {
        Datapoint first = datapoint(1L, 0.0d);
        Datapoint gap = datapoint(3L, 1.0d);
        Datapoint outOfOrder = datapoint(2L, 2.0d);
        send(first);
        send(gap);
        send(outOfOrder);

        verify(processor, timeout(TimeUnit.SECONDS.toMillis(WAIT_SECONDS))).process(first);
        verify(processor, timeout(TimeUnit.SECONDS.toMillis(WAIT_SECONDS))).process(gap);
        verify(processor, timeout(TimeUnit.SECONDS.toMillis(WAIT_SECONDS))).process(outOfOrder);
        await(() -> processorMessages().stream()
                .anyMatch(message -> message.contains("expected=2") && message.contains("actual=3"))
                && processorMessages().stream()
                .anyMatch(message -> message.contains("expected=4") && message.contains("actual=2")));
    }

    @Test
    void retriesThreeTimesThenRepublishesOnePersistentConfirmedDeadLetterAndFreesPrefetch() {
        Datapoint failing = datapoint(1L, 0.0d);
        doThrow(new IllegalStateException("forced processing failure")).when(processor).process(eq(failing));

        send(failing);

        verify(processor, timeout(TimeUnit.SECONDS.toMillis(WAIT_SECONDS)).times(3)).process(failing);
        Message deadLetter = receiveEventually(RabbitTopology.DATAPOINT_DLQ);
        assertThat(deadLetter).isNotNull();
        assertThat(deadLetter.getMessageProperties().getReceivedDeliveryMode()).isEqualTo(MessageDeliveryMode.PERSISTENT);
        assertThat(deadLetter.getMessageProperties().getHeaders())
                .containsKeys("x-exception-message", "x-original-exchange", "x-original-routingKey");
        assertThat(queueDepth(RabbitTopology.DATAPOINT_QUEUE)).isZero();

        Datapoint next = datapoint(2L, 1.0d);
        send(next);
        await(() -> processedCount() == 1.0d);
        assertThat(queueDepth(RabbitTopology.DATAPOINT_QUEUE)).isZero();
    }


    @Test
    void remembersBeforeRetrySoAnAlreadyProcessedRedeliveryIsDeduplicated() {
        Datapoint datapoint = datapoint(1L, 0.0d);
        AtomicBoolean firstAttempt = new AtomicBoolean(true);
        doAnswer(invocation -> {
            ProcessingOutcome outcome = (ProcessingOutcome) invocation.callRealMethod();
            if (firstAttempt.getAndSet(false)) {
                throw new IllegalStateException("ack path interrupted");
            }
            return outcome;
        }).when(processor).process(eq(datapoint));

        send(datapoint);

        await(() -> processedCount() == 1.0d && duplicateCount() == 1.0d);
        verify(processor, timeout(TimeUnit.SECONDS.toMillis(WAIT_SECONDS)).times(2)).process(datapoint);
        assertThat(queueDepth(RabbitTopology.DATAPOINT_QUEUE)).isZero();
    }


    @Test
    void retainsMessagesForALateConsumerAndProcessesThemWhenItStarts() {
        stopListeners();
        Datapoint datapoint = datapoint(1L, 0.0d);
        send(datapoint);

        await(() -> queueDepth(RabbitTopology.DATAPOINT_QUEUE) == 1L);
        listenerRegistry.start();
        await(() -> processedCount() == 1.0d);
    }

    @Test
    void recoversTheListenerAfterRabbitBrokerRestart() throws Exception {
        assertThat(RABBIT.execInContainer("rabbitmqctl", "stop_app").getExitCode()).isZero();
        assertThat(RABBIT.execInContainer("rabbitmqctl", "start_app").getExitCode()).isZero();
        assertThat(RABBIT.execInContainer("rabbitmqctl", "status").getExitCode()).isZero();

        Datapoint datapoint = datapoint(1L, 0.0d);
        sendEventually(datapoint);
        await(() -> processedCount() == 1.0d);
    }


    @Test
    void stopsTheListenerGracefullyWithoutLeavingAnUnackedMessage() {
        stopListeners();
        assertThat(listenerRegistry.getListenerContainers()).allSatisfy(container -> assertThat(container.isRunning()).isFalse());

        send(datapoint(1L, 0.0d));

        await(() -> queueDepth(RabbitTopology.DATAPOINT_QUEUE) == 1L);
        assertThat(queueDepth(RabbitTopology.DATAPOINT_DLQ)).isZero();
    }

    private void send(Datapoint datapoint) {
        rabbitTemplate.convertAndSend(RabbitTopology.METRICS_EXCHANGE, RabbitTopology.METRICS_ROUTING_KEY, datapoint);
    }
    private void sendEventually(Datapoint datapoint) {
        await(() -> {
            try {
                send(datapoint);
                return true;
            } catch (RuntimeException unavailable) {
                return false;
            }
        });
    }

    private void sendRaw(String body) {
        MessageProperties properties = new MessageProperties();
        properties.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        properties.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        rabbitTemplate.send(
                RabbitTopology.METRICS_EXCHANGE,
                RabbitTopology.METRICS_ROUTING_KEY,
                new Message(body.getBytes(StandardCharsets.UTF_8), properties)
        );
    }

    private Message receiveEventually(String queueName) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        Message message;
        do {
            message = rabbitTemplate.receive(queueName, 250);
        } while (message == null && System.nanoTime() < deadline);
        return message;
    }

    private long queueDepth(String queueName) {
        return rabbitTemplate.execute(channel -> channel.messageCount(queueName));
    }

    private double processedCount() {
        return meterRegistry.get("anomaly.detector.points.processed").counter().count();
    }

    private double duplicateCount() {
        return meterRegistry.get("anomaly.detector.points.duplicates").counter().count();
    }

    private double rejectedCount() {
        return meterRegistry.get("anomaly.detector.points.rejected").counter().count();
    }

    private List<String> processorMessages() {
        return processorAppender.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
    }

    private List<String> listenerMessages() {
        return listenerAppender.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
    }

    private void stopListeners() {
        listenerRegistry.getListenerContainers().forEach(container -> container.stop());
        await(() -> listenerRegistry.getListenerContainers().stream().noneMatch(container -> container.isRunning()));
    }

    private void await(BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(100L);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError("Interrupted while waiting for RabbitMQ", interrupted);
            }
        }
        assertThat(condition.getAsBoolean()).isTrue();
    }

    private Datapoint datapoint(long sequence, double value) {
        return new Datapoint(UUID.randomUUID(), sequence, "sensor.temperature", value, EMITTED_AT);
    }

    private String datapointJson(UUID id, long sequence, String value) {
        return """
                {"id":"%s","sequence":%d,"metric":"sensor.temperature","value":%s,"emittedAt":"2026-08-05T14:22:07.361Z"}
                """.formatted(id, sequence, value);
    }
}
