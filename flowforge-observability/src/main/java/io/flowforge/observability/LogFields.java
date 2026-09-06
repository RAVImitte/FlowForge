package io.flowforge.observability;

public final class LogFields {
    public static final String ATTEMPT_NUMBER = "attempt_number";
    public static final String CORRELATION_ID = "correlation_id";
    public static final String EVENT_ID = "event_id";
    public static final String FENCING_TOKEN = "fencing_token";
    public static final String HTTP_METHOD = "http_method";
    public static final String HTTP_PATH = "http_path";
    public static final String KAFKA_OFFSET = "kafka_offset";
    public static final String KAFKA_PARTITION = "kafka_partition";
    public static final String KAFKA_TOPIC = "kafka_topic";
    public static final String TASK_EXECUTION_ID = "task_execution_id";
    public static final String TASK_KEY = "task_key";
    public static final String TENANT_ID = "tenant_id";
    public static final String WORKER_ID = "worker_id";
    public static final String WORKFLOW_EXECUTION_ID = "workflow_execution_id";

    private LogFields() {
    }
}
