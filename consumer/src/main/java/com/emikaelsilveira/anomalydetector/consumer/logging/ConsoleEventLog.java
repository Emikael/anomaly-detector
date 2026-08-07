package com.emikaelsilveira.anomalydetector.consumer.logging;

import lombok.experimental.UtilityClass;

/**
 * Logger name bound to the bare {@code %msg%n} console appender in {@code logback-spring.xml}.
 *
 * <p>The appender is attached to this name rather than to a class, because what owns stdout
 * verbatim is an output contract — the R11 verdict line and the periodic summary — not a package
 * location. Routing by class ties a format to a type: the next WARN or ERROR added to a routed
 * class would silently lose its level, logger and thread, which is the failure the logback config
 * exists to avoid. Emitters opt in by asking for this logger; everything else keeps Spring Boot's
 * standard pattern.
 *
 * <p>The literal is duplicated in {@code logback-spring.xml}, which cannot reference a constant.
 * Change both together.
 */
@UtilityClass
public class ConsoleEventLog {

    public static final String NAME = "anomaly.detector.console";
}
