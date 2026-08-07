package com.emikaelsilveira.anomalydetector.consumer.detection;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

@AnalyzeClasses(
        packages = "com.emikaelsilveira.anomalydetector.consumer.detection",
        importOptions = ImportOption.DoNotIncludeTests.class
)
class DetectionArchitectureTest {

    @ArchTest
    static final ArchRule detectionIsFrameworkFree = noClasses()
            .should()
            .dependOnClassesThat()
            .resideInAnyPackage(
                    "org.springframework..",
                    "com.rabbitmq..",
                    "org.apache.qpid..",
                    "org.slf4j..",
                    "org.apache.logging..",
                    "java.util.logging..",
                    "io.micrometer..",
                    "java.time.."
            );
}
