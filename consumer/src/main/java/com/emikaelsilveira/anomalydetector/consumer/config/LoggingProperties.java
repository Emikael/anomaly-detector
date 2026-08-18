package com.emikaelsilveira.anomalydetector.consumer.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/** Binds and validates the consumer's plain or structured console logging mode. */
@ConfigurationProperties(prefix = "app.logging")
@Validated
public record LoggingProperties(
        @NotBlank @Pattern(regexp = "plain|json") String format
) {
}
