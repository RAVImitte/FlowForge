package io.flowforge.controlplane.config;

import io.flowforge.application.audit.SecurityAuditService;
import io.flowforge.application.audit.AuditOutcome;
import io.flowforge.domain.tenancy.TenantId;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class AdministrativeAuditFilterTest {
    @Test
    void failsClosedBeforeMutationWhenTheAttemptCannotBeAudited() throws Exception {
        SecurityAuditService audit = mock(SecurityAuditService.class);
        doThrow(new IllegalStateException("database unavailable"))
                .when(audit).record(any(), any(), any(), any(), any(), any(), any(), any(), any(), any());
        ObjectMapper objectMapper = JsonMapper.builder().build();
        FlowForgeSecurityProperties properties = new FlowForgeSecurityProperties();
        properties.setEnabled(false);
        MockMvc mvc = MockMvcBuilders.standaloneSetup(new MutationProbe())
                .addFilters(
                        new TenantContextFilter(properties, objectMapper),
                        new AdministrativeAuditFilter(audit, objectMapper)
                )
                .build();

        mvc.perform(post("/api/v1/workflows"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.code").value("AUDIT_UNAVAILABLE"));
    }

    @Test
    void recordsTheExactDeadLetterReplayTargetWithoutPayloadData() throws Exception {
        SecurityAuditService audit = mock(SecurityAuditService.class);
        ObjectMapper objectMapper = JsonMapper.builder().build();
        FlowForgeSecurityProperties properties = new FlowForgeSecurityProperties();
        properties.setEnabled(false);
        MockMvc mvc = MockMvcBuilders.standaloneSetup(new MutationProbe())
                .addFilters(
                        new TenantContextFilter(properties, objectMapper),
                        new AdministrativeAuditFilter(audit, objectMapper)
                )
                .build();

        mvc.perform(post("/api/v1/dead-letters/flowforge.task-results.dlq/partitions/3/offsets/91/replay"))
                .andExpect(status().isOk());

        verify(audit).record(
                org.mockito.ArgumentMatchers.eq(TenantId.LOCAL),
                org.mockito.ArgumentMatchers.eq("local-development"),
                org.mockito.ArgumentMatchers.eq("POST_DEAD_LETTER_REPLAY"),
                org.mockito.ArgumentMatchers.eq("TRANSPORT_DLQ_RECORD"),
                org.mockito.ArgumentMatchers.eq("flowforge.task-results.dlq/partitions/3/offsets/91"),
                org.mockito.ArgumentMatchers.eq(AuditOutcome.ATTEMPTED),
                org.mockito.ArgumentMatchers.eq("POST"),
                org.mockito.ArgumentMatchers.eq("/api/v1/dead-letters/flowforge.task-results.dlq/partitions/3/offsets/91/replay"),
                org.mockito.ArgumentMatchers.isNull(),
                any()
        );
    }

    @Test
    void auditsDeadLetterInspectionAsAnAdministrativeRead() throws Exception {
        SecurityAuditService audit = mock(SecurityAuditService.class);
        ObjectMapper objectMapper = JsonMapper.builder().build();
        FlowForgeSecurityProperties properties = new FlowForgeSecurityProperties();
        properties.setEnabled(false);
        MockMvc mvc = MockMvcBuilders.standaloneSetup(new MutationProbe())
                .addFilters(
                        new TenantContextFilter(properties, objectMapper),
                        new AdministrativeAuditFilter(audit, objectMapper)
                )
                .build();

        String path = "/api/v1/dead-letters/flowforge.task-results.dlq/partitions/1/offsets/7";
        mvc.perform(get(path)).andExpect(status().isOk());

        verify(audit).record(
                org.mockito.ArgumentMatchers.eq(TenantId.LOCAL),
                org.mockito.ArgumentMatchers.eq("local-development"),
                org.mockito.ArgumentMatchers.eq("GET_DEAD_LETTER_INSPECT"),
                org.mockito.ArgumentMatchers.eq("TRANSPORT_DLQ_RECORD"),
                org.mockito.ArgumentMatchers.eq("flowforge.task-results.dlq/partitions/1/offsets/7"),
                org.mockito.ArgumentMatchers.eq(AuditOutcome.ATTEMPTED),
                org.mockito.ArgumentMatchers.eq("GET"),
                org.mockito.ArgumentMatchers.eq(path),
                org.mockito.ArgumentMatchers.isNull(),
                any()
        );
    }

    @RestController
    static class MutationProbe {
        @PostMapping("/api/v1/workflows")
        @ResponseStatus(HttpStatus.CREATED)
        void mutate() {
        }

        @PostMapping("/api/v1/dead-letters/{topic}/partitions/{partition}/offsets/{offset}/replay")
        void replay() {
        }

        @GetMapping("/api/v1/dead-letters/{topic}/partitions/{partition}/offsets/{offset}")
        void inspect() {
        }
    }
}
