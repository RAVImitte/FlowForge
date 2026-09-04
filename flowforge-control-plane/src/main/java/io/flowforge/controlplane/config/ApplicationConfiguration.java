package io.flowforge.controlplane.config;

import io.flowforge.application.execution.ExecutionRepository;
import io.flowforge.application.execution.TaskDispatcher;
import io.flowforge.application.execution.WorkflowExecutionService;
import io.flowforge.application.workflow.WorkflowRepository;
import io.flowforge.application.workflow.WorkflowService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.time.Clock;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Configuration
@EnableScheduling
public class ApplicationConfiguration {
    private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationConfiguration.class);

    @Bean
    WorkflowService workflowService(WorkflowRepository repository) {
        return new WorkflowService(repository);
    }

    @Bean
    WorkflowExecutionService workflowExecutionService(
            ExecutionRepository repository,
            TaskDispatcher dispatcher,
            Clock clock
    ) {
        return new WorkflowExecutionService(
                repository,
                dispatcher,
                clock,
                (workItem, failure) -> LOGGER.error(
                        "Could not persist completion for task {} in workflow execution {}",
                        workItem.taskRunId(),
                        workItem.workflowRunId(),
                        failure
                )
        );
    }

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean(destroyMethod = "shutdown")
    ScheduledExecutorService taskHandlerExecutor() {
        return Executors.newScheduledThreadPool(
                4,
                Thread.ofPlatform().name("flowforge-task-handler-", 0).factory()
        );
    }
}
