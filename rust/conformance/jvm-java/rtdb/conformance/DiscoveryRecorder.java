package rtdb.conformance;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;
import org.junit.platform.engine.discovery.ClassNameFilter;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;

public final class DiscoveryRecorder {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) throw new IllegalArgumentException("output and at least one test class root required");
        var arguments = Arrays.asList(args);
        int sourceStart = arguments.indexOf("--clojure-sources");
        int rootEnd = sourceStart < 0 ? arguments.size() : sourceStart;
        var roots = arguments.subList(1, rootEnd).stream().map(Path::of).collect(Collectors.toSet());
        var sources = sourceStart < 0 ? List.<String>of() : arguments.subList(sourceStart + 1, arguments.size());
        var request = LauncherDiscoveryRequestBuilder.request()
            .selectors(DiscoverySelectors.selectClasspathRoots(roots))
            .selectors(sources.stream().map(DiscoverySelectors::selectClasspathResource).toList())
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
        result.put("test_clojure_sources", sources);
        var output = Path.of(args[0]);
        Files.createDirectories(output.toAbsolutePath().getParent());
        Files.writeString(output, new ObjectMapper().writeValueAsString(result) + "\n", StandardOpenOption.CREATE_NEW);
    }
}
