package io.flowforge.controlplane.config;

import io.flowforge.application.workflow.WorkflowRepository;
import io.flowforge.application.workflow.WorkflowService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ApplicationConfiguration {
    @Bean
    WorkflowService workflowService(WorkflowRepository repository) {
        return new WorkflowService(repository);
    }
}
