package io.flowforge.application.recovery;

import java.time.Duration;
import java.util.Optional;

public interface DeadLetterTransport {
    Optional<DeadLetterRecord> read(DeadLetterLocation location, Duration timeout);

    void replay(DeadLetterRecord record, Duration timeout);
}
