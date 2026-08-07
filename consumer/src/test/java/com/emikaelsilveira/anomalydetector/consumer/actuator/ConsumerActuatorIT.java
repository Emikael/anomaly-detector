package com.emikaelsilveira.anomalydetector.consumer.actuator;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import com.emikaelsilveira.anomalydetector.consumer.ConsumerApplication;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        classes = ConsumerApplication.class,
        properties = {
                "spring.rabbitmq.listener.simple.auto-startup=false",
                "spring.rabbitmq.connection-timeout=500ms"
        },
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
@Testcontainers
class ConsumerActuatorIT {

    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(2L);
    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(30L);

    @Container
    private static final RabbitMQContainer RABBIT = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:3.13-management-alpine")
    );

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(REQUEST_TIMEOUT)
            .build();

    @Value("${local.server.port}")
    private int port;

    @DynamicPropertySource
    static void rabbitProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.rabbitmq.host", RABBIT::getHost);
        registry.add("spring.rabbitmq.port", RABBIT::getAmqpPort);
        registry.add("spring.rabbitmq.username", RABBIT::getAdminUsername);
        registry.add("spring.rabbitmq.password", RABBIT::getAdminPassword);
    }

    @Test
    void exposesOnlyOperationalEndpointsAndDetectorMetrics() throws Exception {
        assertOperationalEndpoint("/actuator/health");
        assertOperationalEndpoint("/actuator/info");
        HttpResponse<String> prometheus = get("/actuator/prometheus");
        assertThat(prometheus.statusCode()).isEqualTo(HttpStatus.OK.value());
        assertThat(prometheus.body())
                .contains("anomaly_detector_points_processed_total")
                .contains("anomaly_detector_points_anomalies_total")
                .contains("anomaly_detector_window_occupancy")
                .contains("anomaly_detector_processing_seconds_count");

        assertThat(get("/actuator/env").statusCode()).isEqualTo(HttpStatus.NOT_FOUND.value());
        assertThat(get("/actuator/configprops").statusCode()).isEqualTo(HttpStatus.NOT_FOUND.value());
        assertThat(get("/actuator/heapdump").statusCode()).isEqualTo(HttpStatus.NOT_FOUND.value());
    }

    @Test
    void keepsLivenessUpWhileRabbitOutageMakesReadinessDownThenRecovers() throws Exception {
        HttpResponse<String> readiness = awaitStatus("/actuator/health/readiness", HttpStatus.OK.value());
        assertThat(readiness.body()).contains("\"rabbit\":{").contains("\"status\":\"UP\"");

        HttpResponse<String> liveness = get("/actuator/health/liveness");
        assertThat(liveness.statusCode()).isEqualTo(HttpStatus.OK.value());
        assertThat(liveness.body()).contains("\"status\":\"UP\"").doesNotContain("rabbit");

        assertThat(RABBIT.execInContainer("rabbitmqctl", "stop_app").getExitCode()).isZero();
        try {
            HttpResponse<String> unavailable = awaitStatus(
                    "/actuator/health/readiness", HttpStatus.SERVICE_UNAVAILABLE.value()
            );
            assertThat(unavailable.body()).contains("\"status\":\"DOWN\"");

            HttpResponse<String> duringOutage = get("/actuator/health/liveness");
            assertThat(duringOutage.statusCode()).isEqualTo(HttpStatus.OK.value());
            assertThat(duringOutage.body()).contains("\"status\":\"UP\"").doesNotContain("rabbit");
        } finally {
            assertThat(RABBIT.execInContainer("rabbitmqctl", "start_app").getExitCode()).isZero();
        }

        HttpResponse<String> recovered = awaitStatus("/actuator/health/readiness", HttpStatus.OK.value());
        assertThat(recovered.body()).contains("\"rabbit\":{").contains("\"status\":\"UP\"");
    }

    private void assertOperationalEndpoint(String path) throws Exception {
        assertThat(get(path).statusCode()).isEqualTo(HttpStatus.OK.value());
    }

    private HttpResponse<String> awaitStatus(String path, int expectedStatus) throws Exception {
        HttpResponse<String> response = null;
        long deadline = System.nanoTime() + AWAIT_TIMEOUT.toNanos();
        do {
            response = get(path);
            if (response.statusCode() == expectedStatus) {
                return response;
            }
            Thread.sleep(200L);
        } while (System.nanoTime() < deadline);

        assertThat(response).isNotNull();
        assertThat(response.statusCode()).isEqualTo(expectedStatus);
        return response;
    }

    private HttpResponse<String> get(String path) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .GET()
                .timeout(REQUEST_TIMEOUT)
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
