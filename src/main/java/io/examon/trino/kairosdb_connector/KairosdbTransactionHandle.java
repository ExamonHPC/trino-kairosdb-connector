package io.examon.trino.kairosdb_connector;

import io.trino.spi.connector.ConnectorTransactionHandle;

/**
 * The KairosDB connector is read-only and stateless, so a single instance is
 * enough to represent every transaction Trino opens against it.
 */
public enum KairosdbTransactionHandle
        implements ConnectorTransactionHandle
{
    INSTANCE
}
