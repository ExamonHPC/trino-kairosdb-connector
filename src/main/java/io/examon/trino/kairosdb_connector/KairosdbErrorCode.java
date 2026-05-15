package io.examon.trino.kairosdb_connector;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.spi.ErrorType.USER_ERROR;

public enum KairosdbErrorCode
        implements ErrorCodeSupplier
{
    KAIROSDB_UNKNOWN_ERROR(0, EXTERNAL),
    KAIROSDB_METRICS_RETRIEVE_ERROR(1, EXTERNAL),
    KAIROSDB_PARSE_ERROR(2, EXTERNAL),
    KAIROSDB_BAD_REQUEST(3, USER_ERROR);

    private final ErrorCode errorCode;

    KairosdbErrorCode(int code, ErrorType type)
    {
        // 0x0510_0000 is the next free 16-bit error-code block after the
        // built-in Prometheus connector (0x0509_0000).
        this.errorCode = new ErrorCode(code + 0x0510_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
