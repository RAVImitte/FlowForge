package io.flowforge.controlplane;

import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

class ArchitectureTest {
    @Test
    void domainAndApplicationDoNotDependOnAdaptersOrFrameworks() {
        var classes = new ClassFileImporter().importPackages("io.flowforge");

        noClasses()
                .that().resideInAnyPackage("io.flowforge.domain..", "io.flowforge.application..")
                .should().dependOnClassesThat()
                .resideInAnyPackage(
                        "io.flowforge.controlplane..",
                        "org.springframework..",
                        "jakarta.."
                )
                .check(classes);
    }
}
