package io.flowforge.controlplane.adapter.out.execution;

import io.flowforge.application.execution.WorkflowExecutionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

@Component
@ConditionalOnProperty(
        name = "flowforge.execution.dispatch-enabled",
        havingValue = "true",
        matchIfMissing = true
)
public class ExecutionDispatchLoop implements ApplicationRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionDispatchLoop.class);
    private static final int CLAIM_BATCH_SIZE = 100;

    private final WorkflowExecutionService service;
    private final AtomicBoolean dispatching = new AtomicBoolean();

    public ExecutionDispatchLoop(WorkflowExecutionService service) {
        this.service = service;
    }

    @Override
    public void run(ApplicationArguments args) {
        dispatchReadyWork();
    }

    @Scheduled(fixedDelayString = "${flowforge.execution.dispatch-interval-ms:250}")
    public void dispatchReadyWork() {
        if (!dispatching.compareAndSet(false, true)) return;
        try {
            int claimed = service.dispatchReadyTasks(CLAIM_BATCH_SIZE);
            if (claimed > 0) LOGGER.debug("Claimed {} ready tasks", claimed);
        } catch (RuntimeException failure) {
            LOGGER.error("Ready-task dispatch failed", failure);
        } finally {
            dispatching.set(false);
        }
    }
}
