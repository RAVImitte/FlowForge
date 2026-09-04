package io.flowforge.controlplane.adapter.in.web;

import io.flowforge.application.execution.ExecutionRepository;
import io.flowforge.application.execution.TaskDispatcher;
import io.flowforge.application.execution.WorkflowExecutionService;
import io.flowforge.domain.execution.WorkflowExecution;
import io.flowforge.domain.execution.WorkflowRun;
import io.flowforge.domain.execution.WorkflowRunStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class WorkflowExecutionControllerTest {
    private static final Instant NOW = Instant.parse("2026-09-03T12:00:00Z");
    private static final UUID WORKFLOW_ID = UUID.fromString("10000000-0000-0000-0000-000000000001");
    private static final UUID EXECUTION_ID = UUID.fromString("20000000-0000-0000-0000-000000000001");

    private ExecutionRepository repository;
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        repository = mock(ExecutionRepository.class);
        TaskDispatcher dispatcher = mock(TaskDispatcher.class);
        WorkflowExecutionService service = new WorkflowExecutionService(
                repository,
                dispatcher,
                Clock.fixed(NOW, ZoneOffset.UTC),
                (workItem, failure) -> { }
        );
        when(repository.claimReadyTasks(anyInt(), any())).thenReturn(List.of());
        mvc = MockMvcBuilders.standaloneSetup(new WorkflowExecutionController(service))
                .setControllerAdvice(new ApiExceptionHandler())
                .build();
    }

    @Test
    void startsAnExecutionAndReturnsItsCanonicalLocation() throws Exception {
        WorkflowExecution execution = execution();
        when(repository.start(WORKFLOW_ID, "order-42", NOW)).thenReturn(execution);
        when(repository.findById(EXECUTION_ID)).thenReturn(Optional.of(execution));

        mvc.perform(post("/api/v1/workflows/{workflowId}/executions", WORKFLOW_ID)
                        .header("Idempotency-Key", "order-42"))
                .andExpect(status().isAccepted())
                .andExpect(header().string("Location", "/api/v1/executions/" + EXECUTION_ID))
                .andExpect(jsonPath("$.id").value(EXECUTION_ID.toString()))
                .andExpect(jsonPath("$.status").value("RUNNING"));
    }

    @Test
    void requiresAnIdempotencyKey() throws Exception {
        mvc.perform(post("/api/v1/workflows/{workflowId}/executions", WORKFLOW_ID))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));
    }

    @Test
    void reportsUnknownExecutionsAsNotFound() throws Exception {
        when(repository.findById(EXECUTION_ID)).thenReturn(Optional.empty());

        mvc.perform(get("/api/v1/executions/{executionId}", EXECUTION_ID))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("EXECUTION_NOT_FOUND"));
    }

    @Test
    void cancelsAnExecution() throws Exception {
        WorkflowRun cancelledRun = execution().workflow()
                .transitionTo(WorkflowRunStatus.CANCELLING, NOW)
                .transitionTo(WorkflowRunStatus.CANCELLED, NOW);
        WorkflowExecution cancelled = new WorkflowExecution(cancelledRun, List.of(), List.of(), List.of());
        when(repository.cancel(EXECUTION_ID, NOW)).thenReturn(cancelled);

        mvc.perform(post("/api/v1/executions/{executionId}/cancel", EXECUTION_ID))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCELLED"));
    }

    private static WorkflowExecution execution() {
        WorkflowRun workflow = WorkflowRun.pending(EXECUTION_ID, WORKFLOW_ID, 1, NOW)
                .transitionTo(WorkflowRunStatus.RUNNING, NOW);
        return new WorkflowExecution(workflow, List.of(), List.of(), List.of());
    }
}
