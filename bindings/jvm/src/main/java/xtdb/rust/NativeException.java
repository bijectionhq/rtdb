package xtdb.rust;

import java.util.List;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.adbc.core.ErrorDetail;

public final class NativeException extends AdbcException {
    private final String category;
    private final String code;
    private final byte[] transit;

    NativeException(String category, String code, String message, byte[] transit) {
        super(message, null, status(category), null, 0, List.of(new ErrorDetail("xtdb.error/category", category), new ErrorDetail("xtdb.error/code", code), new ErrorDetail("xtdb.error/transit+msgpack", transit.clone())));
        this.category = category;
        this.code = code;
        this.transit = transit.clone();
    }
    public String category() { return category; }
    public String code() { return code; }
    public byte[] transit() { return transit.clone(); }
    private static AdbcStatusCode status(String category) {
        return switch (category) {
            case "incorrect" -> AdbcStatusCode.INVALID_ARGUMENT;
            case "unsupported" -> AdbcStatusCode.NOT_IMPLEMENTED;
            case "not-found" -> AdbcStatusCode.NOT_FOUND;
            case "forbidden" -> AdbcStatusCode.UNAUTHORIZED;
            case "conflict" -> AdbcStatusCode.INVALID_STATE;
            case "interrupted" -> AdbcStatusCode.CANCELLED;
            case "unavailable", "busy" -> AdbcStatusCode.IO;
            default -> AdbcStatusCode.INTERNAL;
        };
    }
}
