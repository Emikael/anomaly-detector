package com.emikaelsilveira.anomalydetector.consumer.logging;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@ConfigurationProperties(prefix = "app.logging")
@Validated
public record LoggingProperties(
        @NotBlank @Pattern(regexp = "plain|json") String format
) {
}
