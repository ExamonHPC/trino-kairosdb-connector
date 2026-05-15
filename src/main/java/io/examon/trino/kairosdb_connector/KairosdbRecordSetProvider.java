package io.examon.trino.kairosdb_connector;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordSet;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class KairosdbRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final KairosdbClient client;

    @Inject
    public KairosdbRecordSetProvider(KairosdbClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public RecordSet getRecordSet(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<? extends ColumnHandle> columns)
    {
        KairosdbSplit kairosSplit = (KairosdbSplit) split;
        ImmutableList.Builder<KairosdbColumnHandle> projected = ImmutableList.builder();
        for (ColumnHandle column : columns) {
            projected.add((KairosdbColumnHandle) column);
        }
        return new KairosdbRecordSet(client, kairosSplit, projected.build());
    }
}
