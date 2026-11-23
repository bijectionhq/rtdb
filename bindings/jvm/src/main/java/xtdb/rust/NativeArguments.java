package xtdb.rust;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

final class NativeArguments {
    private NativeArguments() {}

    static VectorSchemaRoot create(BufferAllocator allocator, Map<?, ?> arguments) {
        var fields = new ArrayList<Field>(arguments.size());
        var values = new ArrayList<>(arguments.size());
        for (var entry : arguments.entrySet()) {
            if (!(entry.getKey() instanceof String name) || name.indexOf(0) >= 0 || !encodable(name)) return null;
            var field = field(name, entry.getValue());
            if (field == null) return null;
            fields.add(field);
            values.add(entry.getValue());
        }
        var root = VectorSchemaRoot.create(new Schema(fields), allocator);
        try {
            int column = 0;
            for (Object value : values) {
                var vector = root.getVector(column++);
                vector.setInitialCapacity(1);
                if (vector instanceof ListVector list) {
                    var components = (List<?>) value;
                    list.setInitialCapacity(1, Math.max(1, components.size()));
                    list.allocateNew();
                    list.startNewValue(0);
                    int index = 0;
                    for (Object component : components) write(list.getDataVector(), index++, component);
                    list.endValue(0, components.size());
                } else {
                    vector.allocateNew();
                    write(vector, 0, value);
                }
            }
            root.setRowCount(1);
            return root;
        } catch (Throwable error) {
            try { root.close(); } catch (Throwable close) { error.addSuppressed(close); }
            throw error;
        }
    }

    private static Field field(String name, Object value) {
        if (value instanceof List<?> values && !values.isEmpty()) {
            ArrowType element = null;
            boolean nullable = false;
            for (Object component : values) {
                if (component == null) { nullable = true; continue; }
                var type = scalarType(component);
                if (type == null || (element != null && !element.equals(type))) return null;
                element = type;
            }
            if (element == null) return null;
            var child = new Field("$data$", new FieldType(nullable, element, null), null);
            return new Field(name, FieldType.notNullable(ArrowType.List.INSTANCE), List.of(child));
        }
        var type = scalarType(value);
        return type == null ? null : new Field(name, new FieldType(value == null, type, null), null);
    }

    private static boolean encodable(String text) {
        return text.codePoints().noneMatch(code -> code >= Character.MIN_SURROGATE && code <= Character.MAX_SURROGATE);
    }

    private static ArrowType scalarType(Object value) {
        if (value == null) return MinorType.NULL.getType();
        if (value instanceof Boolean) return MinorType.BIT.getType();
        if (value instanceof Byte) return MinorType.TINYINT.getType();
        if (value instanceof Short) return MinorType.SMALLINT.getType();
        if (value instanceof Integer) return MinorType.INT.getType();
        if (value instanceof Long) return MinorType.BIGINT.getType();
        if (value instanceof Float number && Float.isFinite(number)) return MinorType.FLOAT4.getType();
        if (value instanceof Double number && Double.isFinite(number)) return MinorType.FLOAT8.getType();
        if (value instanceof String text && encodable(text)) return MinorType.VARCHAR.getType();
        return null;
    }

    private static void write(FieldVector vector, int index, Object value) {
        if (value == null) return;
        switch (vector) {
            case BitVector v -> v.setSafe(index, (Boolean) value ? 1 : 0);
            case TinyIntVector v -> v.setSafe(index, (Byte) value);
            case SmallIntVector v -> v.setSafe(index, (Short) value);
            case IntVector v -> v.setSafe(index, (Integer) value);
            case BigIntVector v -> v.setSafe(index, (Long) value);
            case Float4Vector v -> v.setSafe(index, (Float) value);
            case Float8Vector v -> v.setSafe(index, (Double) value);
            case VarCharVector v -> v.setSafe(index, ((String) value).getBytes(StandardCharsets.UTF_8));
            default -> throw new IllegalStateException("Unsupported argument vector: " + vector.getField());
        }
    }
}
