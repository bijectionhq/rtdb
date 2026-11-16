package rtdb.conformance;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;
import org.junit.platform.engine.discovery.ClassNameFilter;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;

public final class DiscoveryRecorder {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) throw new IllegalArgumentException("output and at least one test class root required");
        var roots = Arrays.stream(args).skip(1).map(Path::of).collect(Collectors.toSet());
        var request = LauncherDiscoveryRequestBuilder.request()
            .selectors(DiscoverySelectors.selectClasspathRoots(roots))
            .filters(ClassNameFilter.includeClassNamePatterns(".*"))
            .build();
        var plan = LauncherFactory.create().discover(request);
        var descriptors = new ArrayList<Object>();
        for (var root : plan.getRoots()) {
            descriptors.add(JunitRecorder.descriptor(root));
            for (var child : plan.getDescendants(root)) descriptors.add(JunitRecorder.descriptor(child));
        }
        var result = new LinkedHashMap<String, Object>();
        result.put("schema_version", 1);
        result.put("kind", "discovery_only");
        result.put("project", System.getProperty("rtdb.inventory.project"));
        result.put("descriptors", descriptors);
        result.put("test_class_roots", roots.stream().map(Object::toString).sorted().toList());
        var output = Path.of(args[0]);
        Files.createDirectories(output.toAbsolutePath().getParent());
        Files.writeString(output, new ObjectMapper().writeValueAsString(result) + "\n", StandardOpenOption.CREATE_NEW);
    }
}
