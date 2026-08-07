package com.emikaelsilveira.anomalydetector.consumer.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

class LoggingPropertiesTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(LoggingPropertiesConfiguration.class);

    @Test
    void acceptsPlainAndJsonFormats() {
        contextWith("plain").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(LoggingProperties.class).format()).isEqualTo("plain");
        });
        contextWith("json").run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    void rejectsUnsupportedOrBlankLogFormats() {
        assertInvalid("text");
        assertInvalid("");
    }

    private ApplicationContextRunner contextWith(String format) {
        return contextRunner.withPropertyValues("app.logging.format=" + format);
    }

    private void assertInvalid(String format) {
        contextWith(format).run(context -> {
            assertThat(context).hasFailed();
            assertThat(rootCauseMessage(context.getStartupFailure())).contains("format");
        });
    }

    private String rootCauseMessage(Throwable throwable) {
        Throwable rootCause = throwable;
        while (rootCause.getCause() != null) {
            rootCause = rootCause.getCause();
        }
        return rootCause.getMessage();
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(LoggingProperties.class)
    static class LoggingPropertiesConfiguration {
    }
}
