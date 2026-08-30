package io.flowforge.controlplane.adapter.in.web;

final class PreconditionRequiredException extends RuntimeException {
    PreconditionRequiredException() {
        super("The If-Match header is required for this operation");
    }
}
