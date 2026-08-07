package com.emikaelsilveira.anomalydetector.consumer.detection;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

@AnalyzeClasses(
        packages = "com.emikaelsilveira.anomalydetector.consumer.detection",
        importOptions = ImportOption.DoNotIncludeTests.class
)
class DetectionArchitectureTest {

    /**
     * An allowlist, not a denylist. Enumerating forbidden frameworks only catches the ones somebody
     * thought to name; this fails on the next dependency added, including a reach into the consumer's
     * own {@code contract} or {@code messaging} packages.
     */
    @ArchTest
    static final ArchRule detectionDependsOnlyOnTheJdkCore = classes()
            .should()
            .onlyDependOnClassesThat()
            .resideInAnyPackage(
                    "com.emikaelsilveira.anomalydetector.consumer.detection..",
                    "java.lang..",
                    "java.util.."
            )
            .as("detection should depend on nothing but the JDK core and itself");

    /** {@code java.util.logging} and {@code java.time} would slip through the allowlist above. */
    @ArchTest
    static final ArchRule detectionHasNoClockOrLogging = noClasses()
            .should()
            .dependOnClassesThat()
            .resideInAnyPackage("java.time..", "java.util.logging..")
            .as("detection should carry no wall-clock or logging dependency");
}
