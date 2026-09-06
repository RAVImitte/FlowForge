package io.flowforge.messaging;

public final class FlowForgeTopics {
    public static final String TASK_COMMANDS_V1 = "flowforge.task.commands.v1";
    public static final String TASK_COMMANDS_DLQ_V1 = "flowforge.task.commands.dlq.v1";
    public static final String TASK_RESULTS_V1 = "flowforge.task.results.v1";
    public static final String TASK_RESULTS_DLQ_V1 = "flowforge.task.results.dlq.v1";
    public static final String TASK_HEARTBEATS_V1 = "flowforge.task.heartbeats.v1";
    public static final String TASK_HEARTBEATS_DLQ_V1 = "flowforge.task.heartbeats.dlq.v1";
    public static final String EXECUTION_EVENTS_V1 = "flowforge.execution.events.v1";

    public static String deadLetterTopicFor(String sourceTopic) {
        return switch (sourceTopic) {
            case TASK_COMMANDS_V1 -> TASK_COMMANDS_DLQ_V1;
            case TASK_RESULTS_V1 -> TASK_RESULTS_DLQ_V1;
            case TASK_HEARTBEATS_V1 -> TASK_HEARTBEATS_DLQ_V1;
            default -> throw new IllegalArgumentException("No dead-letter topic for " + sourceTopic);
        };
    }

    public static String sourceTopicForDeadLetter(String deadLetterTopic) {
        return switch (deadLetterTopic) {
            case TASK_COMMANDS_DLQ_V1 -> TASK_COMMANDS_V1;
            case TASK_RESULTS_DLQ_V1 -> TASK_RESULTS_V1;
            case TASK_HEARTBEATS_DLQ_V1 -> TASK_HEARTBEATS_V1;
            default -> throw new IllegalArgumentException("Unsupported dead-letter topic " + deadLetterTopic);
        };
    }

    private FlowForgeTopics() {
    }
}
