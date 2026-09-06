package io.flowforge.load;

import tools.jackson.databind.ObjectMapper;

import java.nio.file.Files;
import java.nio.file.Path;

public final class LoadTestMain {
    private LoadTestMain() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: java -jar flowforge-load-test.jar <profile.json> <report.json>");
            System.exit(64);
        }
        ObjectMapper mapper = new ObjectMapper();
        WorkloadProfile profile = WorkloadProfile.load(Path.of(args[0]), mapper);
        LoadTestReport report = new FlowForgeLoadGenerator().run(profile);
        Path output = Path.of(args[1]).toAbsolutePath();
        if (output.getParent() != null) Files.createDirectories(output.getParent());
        String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(report);
        Files.writeString(output, json);
        System.out.println(json);
        if (!report.passed()) System.exit(2);
    }
}
