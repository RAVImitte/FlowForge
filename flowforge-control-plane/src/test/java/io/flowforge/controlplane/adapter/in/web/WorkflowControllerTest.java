package io.flowforge.controlplane.adapter.in.web;

import io.flowforge.application.workflow.WorkflowRepository;
import io.flowforge.application.workflow.WorkflowService;
import io.flowforge.controlplane.config.FlowForgeSecurityProperties;
import io.flowforge.controlplane.config.TenantContextFilter;
import io.flowforge.domain.tenancy.TenantId;
import io.flowforge.domain.workflow.TaskDefinition;
import io.flowforge.domain.workflow.WorkflowDefinition;
import io.flowforge.domain.workflow.WorkflowDraft;
import io.flowforge.domain.workflow.WorkflowLifecycleStatus;
import io.flowforge.domain.workflow.WorkflowVersionStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class WorkflowControllerTest {
    private static final UUID WORKFLOW_ID = UUID.fromString("b5f787b4-6228-43ee-8f77-5be02e7be327");
    private static final String VALID_REQUEST = """
            {
              "name": "Order processing",
              "tasks": [
                {"key": "VALIDATE_ORDER", "name": "Validate order", "type": "NOOP"}
              ],
              "dependencies": []
            }
            """;

    private WorkflowRepository repository;
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        repository = mock(WorkflowRepository.class);
        WorkflowService service = new WorkflowService(repository);
        FlowForgeSecurityProperties properties = new FlowForgeSecurityProperties();
        properties.setEnabled(false);
        mvc = MockMvcBuilders.standaloneSetup(new WorkflowController(service))
                .setControllerAdvice(new ApiExceptionHandler())
                .addFilters(new TenantContextFilter(properties, JsonMapper.builder().build()))
                .build();
    }

    @Test
    void createsAWorkflowAndReturnsLocationAndEtag() throws Exception {
        when(repository.create(eq(TenantId.LOCAL), any())).thenReturn(workflow());

        mvc.perform(post("/api/v1/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID_REQUEST))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", "/api/v1/workflows/" + WORKFLOW_ID))
                .andExpect(header().string("ETag", "\"0\""))
                .andExpect(jsonPath("$.tenantId").value("local"))
                .andExpect(jsonPath("$.versionStatus").value("DRAFT"));
    }

    @Test
    void acceptsAndReturnsAnExplicitReliabilityPolicy() throws Exception {
        String request = """
                {
                  "name": "Reliable order processing",
                  "tasks": [{
                    "key": "PROCESS_PAYMENT",
                    "name": "Process payment",
                    "type": "HTTP",
                    "reliabilityPolicy": {
                      "maxAttempts": 5,
                      "initialBackoffMs": 1000,
                      "backoffMultiplier": 2.0,
                      "maxBackoffMs": 60000,
                      "jitterFactor": 0.25,
                      "attemptTimeoutMs": 30000,
                      "retryableErrorCodes": ["timeout", "gateway_unavailable"]
                    }
                  }],
                  "dependencies": []
                }
                """;
        when(repository.create(eq(TenantId.LOCAL), any())).thenAnswer(invocation -> {
            WorkflowDraft draft = invocation.getArgument(1, WorkflowDraft.class);
            return workflow(draft.tasks().getFirst());
        });

        mvc.perform(post("/api/v1/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(request))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.tasks[0].reliabilityPolicy.maxAttempts").value(5))
                .andExpect(jsonPath("$.tasks[0].reliabilityPolicy.initialBackoffMs").value(1000))
                .andExpect(jsonPath("$.tasks[0].reliabilityPolicy.attemptTimeoutMs").value(30000))
                .andExpect(jsonPath("$.tasks[0].reliabilityPolicy.retryableErrorCodes")
                        .isArray());
    }

    @Test
    void acceptsAndReturnsVersionedConcurrencyPolicies() throws Exception {
        String request = """
                {
                  "name": "Capacity bounded workflow",
                  "maxConcurrentExecutions": 4,
                  "tasks": [{
                    "key": "PROCESS_PAYMENT",
                    "name": "Process payment",
                    "type": "HTTP",
                    "maxConcurrency": 2
                  }],
                  "dependencies": []
                }
                """;
        when(repository.create(eq(TenantId.LOCAL), any())).thenAnswer(invocation -> {
            WorkflowDraft draft = invocation.getArgument(1, WorkflowDraft.class);
            return workflow(draft.maxConcurrentExecutions(), draft.tasks().getFirst());
        });

        mvc.perform(post("/api/v1/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(request))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.maxConcurrentExecutions").value(4))
                .andExpect(jsonPath("$.tasks[0].maxConcurrency").value(2));
    }

    @Test
    void requiresIfMatchForMutation() throws Exception {
        mvc.perform(put("/api/v1/workflows/" + WORKFLOW_ID)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID_REQUEST))
                .andExpect(status().isPreconditionRequired())
                .andExpect(jsonPath("$.code").value("IF_MATCH_REQUIRED"));
    }

    @Test
    void reportsCyclesAsUnprocessableContent() throws Exception {
        String cyclic = """
                {
                  "name": "Cyclic",
                  "tasks": [
                    {"key": "A", "name": "A", "type": "NOOP"},
                    {"key": "B", "name": "B", "type": "NOOP"}
                  ],
                  "dependencies": [
                    {"taskKey": "A", "dependsOnTaskKey": "B"},
                    {"taskKey": "B", "dependsOnTaskKey": "A"}
                  ]
                }
                """;

        mvc.perform(post("/api/v1/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(cyclic))
                .andExpect(status().isUnprocessableContent())
                .andExpect(jsonPath("$.code").value("INVALID_WORKFLOW"))
                .andExpect(jsonPath("$.violations[0]").value("workflow graph must be acyclic"));
    }

    private static WorkflowDefinition workflow() {
        return workflow(new TaskDefinition("VALIDATE_ORDER", "Validate order", "NOOP", Map.of()));
    }

    private static WorkflowDefinition workflow(TaskDefinition task) {
        return workflow(null, task);
    }

    private static WorkflowDefinition workflow(Integer maxConcurrentExecutions, TaskDefinition task) {
        Instant now = Instant.parse("2026-08-28T10:00:00Z");
        return new WorkflowDefinition(
                WORKFLOW_ID,
                TenantId.LOCAL,
                0,
                WorkflowLifecycleStatus.ACTIVE,
                1,
                WorkflowVersionStatus.DRAFT,
                "Order processing",
                null,
                maxConcurrentExecutions,
                List.of(task),
                List.of(),
                now,
                now,
                null
        );
    }
}
