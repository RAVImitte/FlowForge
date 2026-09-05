package io.flowforge.application.coordination;

import java.time.Duration;

public interface EphemeralTokenBucketStore {
    void replaceIfNewer(TokenBucketSnapshot snapshot, Duration ttl);
}
