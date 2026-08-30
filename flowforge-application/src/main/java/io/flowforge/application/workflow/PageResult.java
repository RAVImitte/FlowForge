package io.flowforge.application.workflow;

import java.util.List;

public record PageResult<T>(List<T> items, int page, int size, long totalElements) {
    public PageResult {
        items = List.copyOf(items);
    }
}
