package io.flowforge.controlplane.adapter.in.web;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import java.util.List;
import java.util.Map;

public record WorkflowRequest(
        @NotBlank @Size(max = 200) String name,
        @Size(max = 2000) String description,
        @NotEmpty @Size(max = 1000) List<@Valid TaskRequest> tasks,
        List<@Valid DependencyRequest> dependencies
) {
    public record TaskRequest(
            @NotBlank @Pattern(regexp = "[A-Z][A-Z0-9_]{0,99}") String key,
            @NotBlank @Size(max = 200) String name,
            @NotBlank @Pattern(regexp = "[A-Z][A-Z0-9_.-]{0,99}") String type,
            Map<String, Object> configuration
    ) {
    }

    public record DependencyRequest(
            @NotBlank String taskKey,
            @NotBlank String dependsOnTaskKey
    ) {
    }
}
