package io.flowforge.controlplane.adapter.in.web;

import io.flowforge.application.workflow.WorkflowRepository;
import io.flowforge.application.workflow.WorkflowService;
import io.flowforge.domain.workflow.TaskDefinition;
import io.flowforge.domain.workflow.WorkflowDefinition;
import io.flowforge.domain.workflow.WorkflowLifecycleStatus;
import io.flowforge.domain.workflow.WorkflowVersionStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
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
        mvc = MockMvcBuilders.standaloneSetup(new WorkflowController(service))
                .setControllerAdvice(new ApiExceptionHandler())
                .build();
    }

    @Test
    void createsAWorkflowAndReturnsLocationAndEtag() throws Exception {
        when(repository.create(any())).thenReturn(workflow());

        mvc.perform(post("/api/v1/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID_REQUEST))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", "/api/v1/workflows/" + WORKFLOW_ID))
                .andExpect(header().string("ETag", "\"0\""))
                .andExpect(jsonPath("$.versionStatus").value("DRAFT"));
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
        Instant now = Instant.parse("2026-08-28T10:00:00Z");
        return new WorkflowDefinition(
                WORKFLOW_ID,
                0,
                WorkflowLifecycleStatus.ACTIVE,
                1,
                WorkflowVersionStatus.DRAFT,
                "Order processing",
                null,
                List.of(new TaskDefinition("VALIDATE_ORDER", "Validate order", "NOOP", Map.of())),
                List.of(),
                now,
                now,
                null
        );
    }
}
