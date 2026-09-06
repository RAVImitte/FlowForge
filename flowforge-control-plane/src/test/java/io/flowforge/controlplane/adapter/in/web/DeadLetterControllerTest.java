package io.flowforge.controlplane.adapter.in.web;

import io.flowforge.application.recovery.DeadLetterLocation;
import io.flowforge.application.recovery.DeadLetterRecordSummary;
import io.flowforge.application.recovery.DeadLetterReplayReceipt;
import io.flowforge.application.recovery.DeadLetterReplayService;
import io.flowforge.controlplane.config.FlowForgeSecurityProperties;
import io.flowforge.controlplane.config.TenantContextFilter;
import io.flowforge.domain.tenancy.TenantId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class DeadLetterControllerTest {
    private static final String TOPIC = "flowforge.task-results.dlq";
    private static final String PATH = "/api/v1/dead-letters/" + TOPIC + "/partitions/2/offsets/17";
    private static final UUID IDEMPOTENCY_KEY = UUID.fromString("1e931f74-33ad-4b93-b81b-da8094c3825a");
    private static final Instant FAILED_AT = Instant.parse("2026-09-06T08:30:00Z");

    private DeadLetterReplayService service;
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        service = mock(DeadLetterReplayService.class);
        FlowForgeSecurityProperties properties = new FlowForgeSecurityProperties();
        properties.setEnabled(false);
        mvc = MockMvcBuilders.standaloneSetup(new DeadLetterController(service))
                .setControllerAdvice(new ApiExceptionHandler())
                .addFilters(new TenantContextFilter(properties, JsonMapper.builder().build()))
                .build();
    }

    @Test
    void returnsMetadataAndDigestWithoutReturningThePayload() throws Exception {
        DeadLetterLocation location = new DeadLetterLocation(TOPIC, 2, 17);
        when(service.inspect(TenantId.LOCAL, location)).thenReturn(new DeadLetterRecordSummary(
                TOPIC, 2, 17, "flowforge.task-results", "result-123", TenantId.LOCAL.value(),
                "task-123", "event-123", "correlation-123", "TASK_COMPLETED", "1",
                "java.lang.IllegalStateException", FAILED_AT, 128, "abc123"
        ));

        mvc.perform(get(PATH))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.recordId").value("result-123"))
                .andExpect(jsonPath("$.payloadBytes").value(128))
                .andExpect(jsonPath("$.payloadSha256").value("abc123"))
                .andExpect(jsonPath("$.payload").doesNotExist());
    }

    @Test
    void requiresAnIdempotencyKeyAndPassesTheOperatorReason() throws Exception {
        DeadLetterLocation location = new DeadLetterLocation(TOPIC, 2, 17);
        when(service.replay(TenantId.LOCAL, location, IDEMPOTENCY_KEY, "local-development",
                "dependency recovered; replay approved")).thenReturn(new DeadLetterReplayReceipt(
                IDEMPOTENCY_KEY, "result-123", "flowforge.task-results",
                DeadLetterReplayReceipt.Status.PUBLISHED, FAILED_AT
        ));

        mvc.perform(post(PATH + "/replay")
                        .header("Idempotency-Key", IDEMPOTENCY_KEY)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"reason":"dependency recovered; replay approved"}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("PUBLISHED"));

        verify(service).replay(TenantId.LOCAL, location, IDEMPOTENCY_KEY, "local-development",
                "dependency recovered; replay approved");
    }

    @Test
    void rejectsMalformedIdempotencyKeys() throws Exception {
        mvc.perform(post(PATH + "/replay")
                        .header("Idempotency-Key", "not-a-uuid")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"reason":"dependency recovered; replay approved"}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));
    }
}
