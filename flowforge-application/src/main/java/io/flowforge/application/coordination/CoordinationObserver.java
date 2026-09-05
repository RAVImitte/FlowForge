package io.flowforge.application.coordination;

public interface CoordinationObserver {
    void degraded(String operation);

    void reconciled(int permitCount);
}
