package io.flowforge.worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class FlowForgeWorkerApplication {
    public static void main(String[] args) {
        SpringApplication.run(FlowForgeWorkerApplication.class, args);
    }
}
