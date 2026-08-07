package com.emikaelsilveira.anomalydetector.producer.messaging;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.emikaelsilveira.anomalydetector.producer.ProducerApplication;
import com.emikaelsilveira.anomalydetector.producer.contract.Datapoint;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        classes = ProducerApplication.class,
        properties = "producer.scheduling-enabled=false",
        webEnvironment = SpringBootTest.WebEnvironment.NONE
)
@Testcontainers
class DatapointPublisherIT {

    @Container
    private static final RabbitMQContainer RABBIT = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:3.13-management-alpine")
    );

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private ConnectionFactory connectionFactory;

    @Autowired
    private AmqpAdmin amqpAdmin;

    @DynamicPropertySource
    static void rabbitProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.rabbitmq.host", RABBIT::getHost);
        registry.add("spring.rabbitmq.port", RABBIT::getAmqpPort);
        registry.add("spring.rabbitmq.username", RABBIT::getAdminUsername);
        registry.add("spring.rabbitmq.password", RABBIT::getAdminPassword);
    }

    @AfterEach
    void clearMainQueue() {
        while (rabbitTemplate.receive(RabbitTopology.DATAPOINT_QUEUE) != null) {
        }
    }

    @Test
    void publishesARoutedPersistentDatapoint() {
        Datapoint datapoint = datapoint();
        new DatapointPublisher(rabbitTemplate).publish(datapoint);

        Message message = receiveEventually(RabbitTopology.DATAPOINT_QUEUE);

        assertThat(message).isNotNull();
        assertThat(message.getMessageProperties().getMessageId()).isEqualTo(datapoint.id().toString());
        assertThat(message.getMessageProperties().getReceivedDeliveryMode()).isEqualTo(MessageDeliveryMode.PERSISTENT);
    }

    @Test
    void returnsMandatoryUnroutableMessages() throws Exception {
        CorrelationData correlation = new CorrelationData("unroutable");
        rabbitTemplate.convertAndSend(
                RabbitTopology.METRICS_EXCHANGE,
                "metrics.unroutable",
                datapoint(),
                message -> {
                    message.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                    return message;
                },
                correlation
        );

        CorrelationData.Confirm confirm = correlation.getFuture().get(10, TimeUnit.SECONDS);

        assertThat(confirm.ack()).isTrue();
        assertThat(correlation.getReturned()).isNotNull();
        assertThat(correlation.getReturned().getRoutingKey()).isEqualTo("metrics.unroutable");
    }

    @Test
    void rejectsOverflowWithAPublisherConfirmNack() throws Exception {
        String queueName = "producer.capacity." + UUID.randomUUID();
        String routingKey = "producer.capacity." + UUID.randomUUID();
        Queue queue = new Queue(queueName, true, false, false, Map.of(
                "x-max-length", 1,
                "x-overflow", "reject-publish"
        ));
        amqpAdmin.declareQueue(queue);
        amqpAdmin.declareBinding(BindingBuilder.bind(queue)
                .to(new org.springframework.amqp.core.DirectExchange(RabbitTopology.METRICS_EXCHANGE))
                .with(routingKey));
        try {
            CorrelationData first = publishPersistent(routingKey, "first");
            assertThat(first.getFuture().get(10, TimeUnit.SECONDS).ack()).isTrue();

            CorrelationData second = publishPersistent(routingKey, "second");
            assertThat(second.getFuture().get(10, TimeUnit.SECONDS).ack()).isFalse();
        } finally {
            amqpAdmin.deleteQueue(queueName);
        }
    }

    private CorrelationData publishPersistent(String routingKey, String body) {
        CorrelationData correlation = new CorrelationData(UUID.randomUUID().toString());
        rabbitTemplate.convertAndSend(
                RabbitTopology.METRICS_EXCHANGE,
                routingKey,
                body,
                message -> {
                    message.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                    return message;
                },
                correlation
        );
        return correlation;
    }

    private Message receiveEventually(String queueName) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        Message message;
        do {
            message = rabbitTemplate.receive(queueName, 250);
        } while (message == null && System.nanoTime() < deadline);
        return message;
    }

    private Datapoint datapoint() {
        return new Datapoint(
                UUID.fromString("0f3a9c1e-6b7d-4a2f-9c11-8de4b5a70c93"),
                1041,
                "sensor.temperature",
                100.4213,
                Instant.parse("2026-08-05T14:22:07.361Z")
        );
    }
}
