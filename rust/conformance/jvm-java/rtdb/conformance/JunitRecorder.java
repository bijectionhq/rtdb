package rtdb.conformance;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.reporting.ReportEntry;
import org.junit.platform.engine.support.descriptor.ClassSource;
import org.junit.platform.engine.support.descriptor.MethodSource;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;

public final class JunitRecorder implements TestExecutionListener {
    private final ObjectMapper json = new ObjectMapper();
    private BufferedWriter output;
    private long sequence;
    private boolean failed;

    static Map<String, Object> descriptor(TestIdentifier identifier) {
        var value = new LinkedHashMap<String, Object>();
        value.put("unique_id", identifier.getUniqueId());
        value.put("parent_id", identifier.getParentId().orElse(null));
        value.put("display_name", identifier.getDisplayName());
        value.put("legacy_name", identifier.getLegacyReportingName());
        value.put("is_test", identifier.isTest());
        value.put("is_container", identifier.isContainer());
        value.put("tags", identifier.getTags().stream().map(Object::toString).sorted().toList());
        var id = identifier.getUniqueIdObject();
        var segments = id.getSegments();
        if (identifier.isTest()
                && id.getEngineId().filter("dev.clojurephant.jovial.clojure-test"::equals).isPresent()
                && segments.size() == 3
                && segments.get(1).getType().equals("namespace")
                && segments.get(2).getType().equals("name")) {
            value.put("clojure_var", segments.get(1).getValue() + "/" + segments.get(2).getValue());
        }
        identifier.getSource().ifPresent(source -> {
            var details = new LinkedHashMap<String, Object>();
            details.put("type", source.getClass().getName());
            details.put("representation", source.toString());
            if (source instanceof MethodSource method) {
                details.put("class_name", method.getClassName());
                details.put("method_name", method.getMethodName());
                details.put("parameter_types", method.getMethodParameterTypes());
            } else if (source instanceof ClassSource type) {
                details.put("class_name", type.getClassName());
            }
            value.put("source", details);
        });
        return value;
    }

    private void emit(String kind, Map<String, Object> fields) {
        if (failed || output == null) throw new IllegalStateException("RTDB inventory writer is not open");
        try {
            var event = new LinkedHashMap<String, Object>();
            event.put("sequence", sequence++);
            event.put("event", kind);
            event.putAll(fields);
            output.write(json.writeValueAsString(event));
            output.newLine();
            output.flush();
        } catch (IOException error) {
            failed = true;
            throw new IllegalStateException("RTDB inventory evidence could not be written", error);
        }
    }

    @Override
    public synchronized void testPlanExecutionStarted(TestPlan plan) {
        if (output != null) throw new IllegalStateException("RTDB inventory plan already open");
        try {
            Path directory = Path.of(System.getProperty("rtdb.inventory.output"));
            Files.createDirectories(directory);
            output = Files.newBufferedWriter(directory.resolve(ProcessHandle.current().pid() + "-" + UUID.randomUUID() + ".jsonl"),
                StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            sequence = 0;
            failed = false;
            var configuration = new LinkedHashMap<String, Object>();
            for (String key : plan.getConfigurationParameters().keySet().stream().sorted().toList()) {
                configuration.put(key, plan.getConfigurationParameters().get(key).orElse(null));
            }
            emit("plan_started", Map.of("schema_version", 1,
                "project", System.getProperty("rtdb.inventory.project"),
                "task", System.getProperty("rtdb.inventory.task"),
                "configuration", configuration));
            for (TestIdentifier root : plan.getRoots()) {
                emit("discovered", Map.of("descriptor", descriptor(root)));
                for (TestIdentifier child : plan.getDescendants(root)) {
                    emit("discovered", Map.of("descriptor", descriptor(child)));
                }
            }
        } catch (IOException error) {
            failed = true;
            throw new IllegalStateException("RTDB inventory plan could not be opened", error);
        }
    }

    @Override
    public synchronized void dynamicTestRegistered(TestIdentifier test) {
        emit("dynamic_registered", Map.of("descriptor", descriptor(test)));
    }

    @Override
    public synchronized void executionStarted(TestIdentifier test) {
        emit("execution_started", Map.of("descriptor", descriptor(test)));
    }

    @Override
    public synchronized void executionSkipped(TestIdentifier test, String reason) {
        emit("execution_skipped", Map.of("descriptor", descriptor(test), "reason", reason));
    }

    @Override
    public synchronized void executionFinished(TestIdentifier test, TestExecutionResult result) {
        var fields = new LinkedHashMap<String, Object>();
        fields.put("descriptor", descriptor(test));
        fields.put("status", result.getStatus().name());
        result.getThrowable().ifPresent(error -> {
            fields.put("failure_type", error.getClass().getName());
            fields.put("failure_message", error.getMessage());
            fields.put("failure_stack", java.util.Arrays.stream(error.getStackTrace()).map(Object::toString).toList());
        });
        emit("execution_finished", fields);
    }

    @Override
    public synchronized void reportingEntryPublished(TestIdentifier test, ReportEntry entry) {
        emit("report_entry", Map.of("descriptor", descriptor(test), "entries", entry.getKeyValuePairs(),
            "timestamp", entry.getTimestamp().toString()));
    }

    @Override
    public synchronized void testPlanExecutionFinished(TestPlan plan) {
        try {
            if (!failed) emit("plan_finished", Map.of());
        } finally {
            if (output != null) {
                try { output.close(); }
                catch (IOException error) { throw new IllegalStateException("RTDB inventory plan could not be closed", error); }
                finally { output = null; }
            }
        }
    }
}
