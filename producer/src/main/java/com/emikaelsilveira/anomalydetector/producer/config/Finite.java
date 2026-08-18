package com.emikaelsilveira.anomalydetector.producer.config;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import jakarta.validation.Constraint;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import jakarta.validation.Payload;

/**
 * Rejects {@code NaN} and both infinities.
 *
 * <p>Not redundant with {@code @DecimalMin}/{@code @DecimalMax}: Hibernate Validator deliberately
 * orders {@code NaN} as greater than any minimum and smaller than any maximum, so a bounded field
 * such as {@code anomaly-probability} accepts {@code NaN} through both. A non-finite value would
 * then reach the generator and poison every datapoint it produces.
 */
@Documented
@Constraint(validatedBy = Finite.Validator.class)
@Target({ElementType.FIELD, ElementType.RECORD_COMPONENT})
@Retention(RetentionPolicy.RUNTIME)
public @interface Finite {

    String message() default "must be a finite number";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

    /** Implements the finite-number check used by the {@link Finite} constraint. */
    class Validator implements ConstraintValidator<Finite, Double> {

        /** Null passes, per the Bean Validation convention that presence is {@code @NotNull}'s job. */
        @Override
        public boolean isValid(Double value, ConstraintValidatorContext context) {
            return value == null || Double.isFinite(value);
        }
    }
}
