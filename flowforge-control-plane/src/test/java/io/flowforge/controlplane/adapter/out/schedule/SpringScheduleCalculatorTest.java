package io.flowforge.controlplane.adapter.out.schedule;

import io.flowforge.domain.schedule.CronSchedule;
import io.flowforge.domain.schedule.OneTimeSchedule;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpringScheduleCalculatorTest {
    private static final Instant NOW = Instant.parse("2026-09-05T12:34:56Z");
    private final SpringScheduleCalculator calculator = new SpringScheduleCalculator();

    @Test
    void computesOneTimeAndZoneAwareCronOccurrences() {
        Instant fireAt = NOW.plusSeconds(60);
        assertThat(calculator.nextFireAt(new OneTimeSchedule(fireAt), NOW)).isEqualTo(fireAt);

        assertThat(calculator.nextFireAt(
                new CronSchedule("0 0 9 * * *", ZoneId.of("Asia/Kolkata")),
                NOW
        )).isEqualTo(Instant.parse("2026-09-06T03:30:00Z"));
    }

    @Test
    void rejectsPastOneTimeAndInvalidCronSchedules() {
        assertThatThrownBy(() -> calculator.nextFireAt(new OneTimeSchedule(NOW), NOW))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("future");
        assertThatThrownBy(() -> calculator.nextFireAt(
                new CronSchedule("invalid", ZoneId.of("UTC")), NOW
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid cronExpression");
    }

    @Test
    void handlesDaylightSavingGapsDeterministically() {
        assertThat(calculator.nextFireAt(
                new CronSchedule("0 30 2 * * *", ZoneId.of("America/New_York")),
                Instant.parse("2026-03-08T06:59:00Z")
        )).isEqualTo(Instant.parse("2026-03-09T06:30:00Z"));
    }
}
