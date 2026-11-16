package xtdb.rust;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.TypedKey;
import org.apache.arrow.memory.BufferAllocator;

public final class AdbcDriverFactory implements org.apache.arrow.adbc.drivermanager.AdbcDriverFactory {
    public static final TypedKey<String> XTDB_CONFIG_YAML = new TypedKey<>("xtdb.config-yaml", String.class);
    public static final TypedKey<Path> XTDB_CONFIG_FILE = new TypedKey<>("xtdb.config-path", Path.class);
    public static final TypedKey<Long> NATIVE_MEMORY_BYTES = new TypedKey<>("xtdb.native-memory-bytes", Long.class);
    @Override public AdbcDriver getDriver(BufferAllocator allocator) {
        return options -> {
            if (options.containsKey("xtdb.config")) throw AdbcException.invalidArgument("JVM Xtdb.Config factories require migration to native YAML");
            String yaml = XTDB_CONFIG_YAML.get(options);
            Path path = XTDB_CONFIG_FILE.get(options);
            if (yaml == null && path != null) {
                if (!path.toString().endsWith(".yaml")) throw AdbcException.invalidArgument("Configuration file must end in .yaml");
                try { yaml = Files.readString(path); } catch (java.io.IOException error) { throw AdbcException.io(error.getMessage()).withCause(error); }
            }
            Long limit = NATIVE_MEMORY_BYTES.get(options);
            return NativeDatabase.open(allocator, yaml == null ? "" : yaml, limit == null ? 512L * 1024 * 1024 : limit);
        };
    }
}
