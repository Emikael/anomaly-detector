package com.emikaelsilveira.anomalydetector.producer.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

class ProducerPropertiesTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(ProducerPropertiesConfiguration.class);

    @Test
    void bindsPdrDefaults() {
        contextWith().run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(ProducerProperties.class)).isEqualTo(
                    new ProducerProperties(250, 100.0, 5.0, 0.02, 8.0, 12.0, 42L, false, 400, 10.0));
        });
    }

    @Test
    void bindsConfiguredProducerValues() {
        contextWith(
                "producer.interval-ms=1",
                "producer.mean=-20.5",
                "producer.stddev=2.5",
                "producer.anomaly-probability=1.0",
                "producer.anomaly-sigma-min=3.0",
                "producer.anomaly-sigma-max=4.0",
                "producer.seed=99",
                "producer.level-shift-enabled=true",
                "producer.level-shift-at-sequence=7",
                "producer.level-shift-sigma=6.0"
        ).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(ProducerProperties.class)).isEqualTo(
                    new ProducerProperties(1, -20.5, 2.5, 1.0, 3.0, 4.0, 99L, true, 7, 6.0));
        });
    }

    @Test
    void blankSeedBindsAsNull() {
        contextWith("producer.seed=").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(ProducerProperties.class).seed()).isNull();
        });
    }

    @Test
    void acceptsEveryNumericBoundary() {
        contextWith(
                "producer.interval-ms=1",
                "producer.mean=0.0",
                "producer.stddev=1.0",
                "producer.anomaly-probability=0.0",
                "producer.anomaly-sigma-min=1.0",
                "producer.anomaly-sigma-max=1.0",
                "producer.level-shift-at-sequence=1",
                "producer.level-shift-sigma=0.0"
        ).run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    void rejectsAnIntervalBelowOneMillisecondBeforeRuntimeStarts() {
        assertInvalid("producer.interval-ms=0", "intervalMs");
    }

    @Test
    void rejectsNonFiniteMeanAndStddev() {
        assertInvalid("producer.mean=Infinity", "mean");
        assertInvalid("producer.stddev=NaN", "stddev");
    }

    @Test
    void rejectsAnomalyProbabilityOutsideZeroToOne() {
        assertInvalid("producer.anomaly-probability=-0.01", "anomalyProbability");
        assertInvalid("producer.anomaly-probability=1.01", "anomalyProbability");
        assertInvalid("producer.anomaly-probability=NaN", "anomalyProbability");
    }

    @Test
    void rejectsInvalidAnomalySigmaRange() {
        assertInvalid("producer.anomaly-sigma-min=0.0", "anomalySigmaMin");
        assertInvalid("producer.anomaly-sigma-min=5.0", "producer.anomaly-sigma-max=4.0", "anomalySigmaMin");
    }

    @Test
    void rejectsInvalidLevelShiftConfiguration() {
        assertInvalid("producer.level-shift-at-sequence=0", "levelShiftAtSequence");
        assertInvalid("producer.level-shift-sigma=-0.1", "levelShiftSigma");
        assertInvalid("producer.level-shift-sigma=Infinity", "levelShiftSigma");
    }

    @Test
    void rejectsGeneratedValuesOutsideTheSafeEnvelope() {
        assertInvalid(
                "producer.mean=9.0E149",
                "producer.stddev=1.0E149",
                "producer.anomaly-sigma-max=2.0",
                "producer.level-shift-sigma=1.0",
                "generated values"
        );
    }

    private void assertInvalid(String property, String propertyName) {
        contextWith(property).run(context -> {
            assertThat(context).hasFailed();
            assertThat(rootCauseMessage(context.getStartupFailure())).contains(propertyName);
        });
    }

    private void assertInvalid(String firstProperty, String secondProperty, String propertyName) {
        contextWith(firstProperty, secondProperty).run(context -> {
            assertThat(context).hasFailed();
            assertThat(rootCauseMessage(context.getStartupFailure())).contains(propertyName);
        });
    }
    private void assertInvalid(
            String firstProperty,
            String secondProperty,
            String thirdProperty,
            String fourthProperty,
            String propertyName
    ) {
        contextWith(firstProperty, secondProperty, thirdProperty, fourthProperty).run(context -> {
            assertThat(context).hasFailed();
            assertThat(rootCauseMessage(context.getStartupFailure())).contains(propertyName);
        });
    }


    private String rootCauseMessage(Throwable throwable) {
        Throwable rootCause = throwable;
        while (rootCause.getCause() != null) {
            rootCause = rootCause.getCause();
        }
        return rootCause.getMessage();
    }

    private ApplicationContextRunner contextWith(String... overrides) {
        List<String> properties = new ArrayList<>(List.of(
                "producer.interval-ms=250",
                "producer.mean=100.0",
                "producer.stddev=5.0",
                "producer.anomaly-probability=0.02",
                "producer.anomaly-sigma-min=8.0",
                "producer.anomaly-sigma-max=12.0",
                "producer.seed=42",
                "producer.level-shift-enabled=false",
                "producer.level-shift-at-sequence=400",
                "producer.level-shift-sigma=10.0"
        ));
        properties.addAll(Arrays.asList(overrides));
        return contextRunner.withPropertyValues(properties.toArray(String[]::new));
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(ProducerProperties.class)
    static class ProducerPropertiesConfiguration {
    }
}
