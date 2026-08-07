package com.emikaelsilveira.anomalydetector.consumer.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.emikaelsilveira.anomalydetector.consumer.ConsumerApplication;
import com.emikaelsilveira.anomalydetector.consumer.contract.Datapoint;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.DefaultJacksonJavaTypeMapper;
import org.springframework.amqp.support.converter.JacksonJavaTypeMapper;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest(
        classes = {ConsumerApplication.class, SpringAmqp4CompatibilityIT.CompatibilityConfiguration.class},
        webEnvironment = SpringBootTest.WebEnvironment.NONE
)
@Testcontainers
class SpringAmqp4CompatibilityIT {

    private static final String PRODUCER_DATAPOINT_FQCN =
            "com.emikaelsilveira.anomalydetector.producer.contract.Datapoint";
    private static final String COMPATIBILITY_QUEUE = "compatibility.datapoint.q";
    private static final String COMPATIBILITY_ROUTING_KEY = "compatibility.datapoint";

    @Container
    private static final RabbitMQContainer RABBIT = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:3.13-management-alpine")
    );

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private JacksonJsonMessageConverter converter;

    @Autowired
    private CompatibilityListener listener;

    @DynamicPropertySource
    static void rabbitProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.rabbitmq.host", RABBIT::getHost);
        registry.add("spring.rabbitmq.port", RABBIT::getAmqpPort);
        registry.add("spring.rabbitmq.username", RABBIT::getAdminUsername);
        registry.add("spring.rabbitmq.password", RABBIT::getAdminPassword);
    }

    @Test
    void foreignProducerTypeHeaderIsIgnoredForInferredConsumerParameter() throws InterruptedException {
        assertThat(converter.getTypePrecedence()).isEqualTo(JacksonJavaTypeMapper.TypePrecedence.INFERRED);

        Datapoint expected = new Datapoint(
                UUID.fromString("0f3a9c1e-6b7d-4a2f-9c11-8de4b5a70c93"),
                1041,
                "sensor.temperature",
                100.4213,
                Instant.parse("2026-08-05T14:22:03.114Z")
        );
        Message message = converter.toMessage(expected, new MessageProperties());
        message.getMessageProperties().setHeader(
                DefaultJacksonJavaTypeMapper.DEFAULT_CLASSID_FIELD_NAME,
                PRODUCER_DATAPOINT_FQCN
        );

        Object typeId = message.getMessageProperties().getHeader(DefaultJacksonJavaTypeMapper.DEFAULT_CLASSID_FIELD_NAME);
        assertThat(typeId).isEqualTo(PRODUCER_DATAPOINT_FQCN);

        rabbitTemplate.send(RabbitTopology.METRICS_EXCHANGE, COMPATIBILITY_ROUTING_KEY, message);

        assertThat(listener.await()).isEqualTo(expected);
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class CompatibilityConfiguration {

        @Bean
        Queue compatibilityQueue() {
            return new Queue(COMPATIBILITY_QUEUE, true);
        }

        @Bean
        Binding compatibilityBinding(
                @Qualifier("compatibilityQueue") Queue queue,
                @Qualifier("metricsExchange") DirectExchange exchange
        ) {
            return BindingBuilder.bind(queue).to(exchange).with(COMPATIBILITY_ROUTING_KEY);
        }

        @Bean
        CompatibilityListener compatibilityListener() {
            return new CompatibilityListener();
        }

    }

    static class CompatibilityListener {

        private final CountDownLatch received = new CountDownLatch(1);
        private volatile Datapoint datapoint;

        @RabbitListener(id = "compatibilityTypePrecedenceListener", queues = COMPATIBILITY_QUEUE)
        public void receive(Datapoint datapoint) {
            this.datapoint = datapoint;
            received.countDown();
        }

        Datapoint await() throws InterruptedException {
            return received.await(10, TimeUnit.SECONDS) ? datapoint : null;
        }
    }
}
