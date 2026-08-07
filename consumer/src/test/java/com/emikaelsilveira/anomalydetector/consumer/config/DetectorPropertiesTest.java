package com.emikaelsilveira.anomalydetector.consumer.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

class DetectorPropertiesTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(DetectorPropertiesConfiguration.class);

    @Test
    void bindsConfiguredDetectorValues() {
        contextWith("detector.window-size=100", "detector.z-threshold=3.5", "detector.min-samples=50",
                "detector.exclude-anomalies=false", "detector.consecutive-override=10", "detector.summary-every=7")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(DetectorProperties.class)).isEqualTo(
                            new DetectorProperties(100, 3.5, 50, false, 10, 7));
                });
    }

    @Test
    void acceptsEveryLowerBoundary() {
        contextWith("detector.window-size=50", "detector.z-threshold=0.5", "detector.min-samples=2",
                "detector.consecutive-override=2", "detector.summary-every=1")
                .run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    void acceptsEveryUpperBoundary() {
        contextWith("detector.window-size=100", "detector.min-samples=100", "detector.consecutive-override=100")
                .run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    void rejectsWindowSizeBelowFifty() {
        assertInvalid("detector.window-size=49", "windowSize");
    }

    @Test
    void rejectsWindowSizeAboveOneHundred() {
        assertInvalid("detector.window-size=101", "windowSize");
    }

    @Test
    void rejectsThresholdBelowHalf() {
        assertInvalid("detector.z-threshold=0.49", "zThreshold");
    }

    @Test
    void rejectsNonFiniteThreshold() {
        assertInvalid("detector.z-threshold=Infinity", "zThreshold");
    }

    @Test
    void rejectsMinimumSamplesOutsideItsRange() {
        assertInvalid("detector.min-samples=1", "minSamples");
    }

    @Test
    void rejectsMinimumSamplesAboveTheWindow() {
        assertInvalid("detector.window-size=50", "detector.min-samples=51", "minSamples");
    }

    @Test
    void rejectsConsecutiveOverrideOutsideItsRange() {
        assertInvalid("detector.consecutive-override=1", "consecutiveOverride");
        assertInvalid("detector.window-size=50", "detector.consecutive-override=51", "consecutiveOverride");
    }

    @Test
    void rejectsSummaryCadenceBelowOne() {
        assertInvalid("detector.summary-every=0", "summaryEvery");
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
    private String rootCauseMessage(Throwable throwable) {
        Throwable rootCause = throwable;
        while (rootCause.getCause() != null) {
            rootCause = rootCause.getCause();
        }
        return rootCause.getMessage();
    }


    private ApplicationContextRunner contextWith(String... overrides) {
        List<String> values = new ArrayList<>(List.of(
                "detector.window-size=50",
                "detector.z-threshold=3.0",
                "detector.min-samples=30",
                "detector.exclude-anomalies=true",
                "detector.consecutive-override=5",
                "detector.summary-every=100"
        ));
        values.addAll(Arrays.asList(overrides));
        return contextRunner.withPropertyValues(values.toArray(String[]::new));
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(DetectorProperties.class)
    static class DetectorPropertiesConfiguration {
    }
}
