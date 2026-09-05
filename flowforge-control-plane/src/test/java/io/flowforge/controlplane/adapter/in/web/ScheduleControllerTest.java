package io.flowforge.controlplane.adapter.in.web;

import io.flowforge.application.schedule.ScheduleCalculator;
import io.flowforge.application.schedule.ScheduleRepository;
import io.flowforge.application.schedule.WorkflowScheduleService;
import io.flowforge.application.workflow.WorkflowRepository;
import io.flowforge.domain.schedule.CronSchedule;
import io.flowforge.domain.schedule.MisfirePolicy;
import io.flowforge.domain.schedule.ScheduleStatus;
import io.flowforge.domain.schedule.WorkflowSchedule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class ScheduleControllerTest {
    private static final Instant NOW = Instant.parse("2026-09-05T12:00:00Z");
    private static final UUID WORKFLOW_ID = UUID.fromString("f7b43fb3-4e7f-4fca-8971-70cfd46d4977");
    private static final UUID SCHEDULE_ID = UUID.fromString("35cb0805-f62a-438e-951f-2bf56082888c");

    private ScheduleRepository schedules;
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        schedules = mock(ScheduleRepository.class);
        WorkflowRepository workflows = mock(WorkflowRepository.class);
        ScheduleCalculator calculator = mock(ScheduleCalculator.class);
        when(workflows.findById(WORKFLOW_ID)).thenReturn(Optional.of(mock()));
        when(workflows.hasPublishedVersion(WORKFLOW_ID)).thenReturn(true);
        when(calculator.nextFireAt(any(), any())).thenReturn(NOW.plusSeconds(3600));
        WorkflowScheduleService service = new WorkflowScheduleService(
                schedules, workflows, calculator, Clock.fixed(NOW, ZoneOffset.UTC)
        );
        mvc = MockMvcBuilders.standaloneSetup(new ScheduleController(service))
                .setControllerAdvice(new ApiExceptionHandler())
                .build();
    }

    @Test
    void createsAZoneAwareCronScheduleWithLocationAndEtag() throws Exception {
        when(schedules.create(any(), any(), any())).thenReturn(schedule());

        mvc.perform(post("/api/v1/schedules")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "workflowId": "%s",
                                  "type": "CRON",
                                  "cronExpression": "0 0 9 * * *",
                                  "timeZone": "Asia/Kolkata",
                                  "misfirePolicy": "FIRE_ONCE"
                                }
                                """.formatted(WORKFLOW_ID)))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", "/api/v1/schedules/" + SCHEDULE_ID))
                .andExpect(header().string("ETag", "\"0\""))
                .andExpect(jsonPath("$.type").value("CRON"))
                .andExpect(jsonPath("$.timeZone").value("Asia/Kolkata"))
                .andExpect(jsonPath("$.status").value("ACTIVE"));
    }

    @Test
    void rejectsFieldsFromTheWrongScheduleVariant() throws Exception {
        mvc.perform(post("/api/v1/schedules")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "workflowId": "%s",
                                  "type": "ONE_TIME",
                                  "fireAt": "2026-09-06T12:00:00Z",
                                  "cronExpression": "0 0 9 * * *"
                                }
                                """.formatted(WORKFLOW_ID)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));
    }

    @Test
    void requiresIfMatchForPause() throws Exception {
        mvc.perform(post("/api/v1/schedules/" + SCHEDULE_ID + "/pause"))
                .andExpect(status().isPreconditionRequired())
                .andExpect(jsonPath("$.code").value("IF_MATCH_REQUIRED"));
    }

    private static WorkflowSchedule schedule() {
        return new WorkflowSchedule(
                SCHEDULE_ID,
                WORKFLOW_ID,
                new CronSchedule("0 0 9 * * *", ZoneId.of("Asia/Kolkata")),
                MisfirePolicy.FIRE_ONCE,
                ScheduleStatus.ACTIVE,
                NOW.plusSeconds(3600),
                0,
                NOW,
                NOW
        );
    }
}
