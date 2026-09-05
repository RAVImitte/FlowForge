package io.flowforge.controlplane.config;

import io.flowforge.application.coordination.CoordinationObserver;
import io.flowforge.application.coordination.CoordinationPermitLedger;
import io.flowforge.application.coordination.CoordinationPermitService;
import io.flowforge.application.coordination.EphemeralPermitStore;
import io.flowforge.application.coordination.TokenBucketPolicy;
import io.flowforge.application.coordination.TokenBucketRateLimiter;
import io.flowforge.application.execution.AdmissionBackpressureObserver;
import io.flowforge.application.execution.ExecutionRepository;
import io.flowforge.application.execution.TaskDispatcher;
import io.flowforge.application.execution.WorkflowExecutionService;
import io.flowforge.application.schedule.ScheduleCalculator;
import io.flowforge.application.schedule.ScheduleFireRepository;
import io.flowforge.application.schedule.ScheduleFireService;
import io.flowforge.application.schedule.ScheduleRepository;
import io.flowforge.application.schedule.ScheduleBackpressureObserver;
import io.flowforge.application.schedule.WorkflowScheduleService;
import io.flowforge.application.workflow.WorkflowRepository;
import io.flowforge.application.workflow.WorkflowService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
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
    @ConditionalOnProperty(prefix = "flowforge.coordination", name = "enabled", havingValue = "true")
    CoordinationPermitService coordinationPermitService(
            CoordinationPermitLedger ledger,
            EphemeralPermitStore ephemeralStore,
            CoordinationObserver observer,
            Clock clock,
            @Value("${flowforge.coordination.lease-duration:30s}") Duration leaseDuration,
            @Value("${flowforge.coordination.ttl-padding:2m}") Duration ttlPadding
    ) {
        return new CoordinationPermitService(
                ledger,
                ephemeralStore,
                observer,
                clock,
                leaseDuration,
                ttlPadding
        );
    }

    @Bean
    ScheduleFireService scheduleFireService(
            ScheduleFireRepository fires,
            WorkflowExecutionService executions,
            Clock clock,
            @Value("${flowforge.scheduling.instance-id:${spring.application.name}-${random.uuid}}") String instanceId,
            @Value("${flowforge.scheduling.lease-duration:30s}") Duration leaseDuration,
            @Value("${flowforge.scheduling.retry-delay:5s}") Duration retryDelay,
            @Value("${flowforge.scheduling.misfire-threshold:1m}") Duration misfireThreshold,
            @Value("${flowforge.backpressure.max-pending-schedule-fires:10000}") int maxPending,
            TokenBucketRateLimiter rateLimiter,
            ScheduleBackpressureObserver backpressureObserver,
            @Value("${flowforge.rate-limits.schedule-fires.capacity:100}") int capacity,
            @Value("${flowforge.rate-limits.schedule-fires.refill-tokens:100}") int refillTokens,
            @Value("${flowforge.rate-limits.schedule-fires.refill-period:1s}") Duration refillPeriod
    ) {
        return new ScheduleFireService(
                fires,
                executions,
                clock,
                instanceId,
                leaseDuration,
                retryDelay,
                misfireThreshold,
                maxPending,
                rateLimiter,
                new TokenBucketPolicy(capacity, refillTokens, refillPeriod),
                backpressureObserver
        );
    }

    @Bean
    WorkflowExecutionService workflowExecutionService(
            ExecutionRepository repository,
            TaskDispatcher dispatcher,
            Clock clock,
            TokenBucketRateLimiter rateLimiter,
            AdmissionBackpressureObserver backpressureObserver,
            @Value("${flowforge.rate-limits.task-dispatch.capacity:200}") int capacity,
            @Value("${flowforge.rate-limits.task-dispatch.refill-tokens:200}") int refillTokens,
            @Value("${flowforge.rate-limits.task-dispatch.refill-period:1s}") Duration refillPeriod
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
                ),
                rateLimiter,
                new TokenBucketPolicy(capacity, refillTokens, refillPeriod),
                backpressureObserver
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
