package io.flowforge.controlplane.config;

import io.flowforge.application.execution.ExecutionRepository;
import io.flowforge.application.execution.TaskDispatcher;
import io.flowforge.application.execution.WorkflowExecutionService;
import io.flowforge.application.schedule.ScheduleCalculator;
import io.flowforge.application.schedule.ScheduleFireRepository;
import io.flowforge.application.schedule.ScheduleFireService;
import io.flowforge.application.schedule.ScheduleRepository;
import io.flowforge.application.schedule.WorkflowScheduleService;
import io.flowforge.application.workflow.WorkflowRepository;
import io.flowforge.application.workflow.WorkflowService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.time.Clock;
import java.time.Duration;
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
    WorkflowScheduleService workflowScheduleService(
            ScheduleRepository schedules,
            WorkflowRepository workflows,
            ScheduleCalculator calculator,
            Clock clock
    ) {
        return new WorkflowScheduleService(schedules, workflows, calculator, clock);
    }

    @Bean
    ScheduleFireService scheduleFireService(
            ScheduleFireRepository fires,
            WorkflowExecutionService executions,
            Clock clock,
            @Value("${flowforge.scheduling.instance-id:${spring.application.name}-${random.uuid}}") String instanceId,
            @Value("${flowforge.scheduling.lease-duration:30s}") Duration leaseDuration,
            @Value("${flowforge.scheduling.retry-delay:5s}") Duration retryDelay,
            @Value("${flowforge.scheduling.misfire-threshold:1m}") Duration misfireThreshold
    ) {
        return new ScheduleFireService(
                fires,
                executions,
                clock,
                instanceId,
                leaseDuration,
                retryDelay,
                misfireThreshold
        );
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
