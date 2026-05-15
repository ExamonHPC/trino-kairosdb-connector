package io.examon.trino.kairosdb_connector;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class KairosdbRecordSet
        implements RecordSet
{
    private final KairosdbClient client;
    private final KairosdbSplit split;
    private final List<KairosdbColumnHandle> columnHandles;
    private final List<Type> columnTypes;

    public KairosdbRecordSet(KairosdbClient client, KairosdbSplit split, List<KairosdbColumnHandle> columnHandles)
    {
        this.client = requireNonNull(client, "client is null");
        this.split = requireNonNull(split, "split is null");
        this.columnHandles = ImmutableList.copyOf(requireNonNull(columnHandles, "columnHandles is null"));
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (KairosdbColumnHandle handle : columnHandles) {
            types.add(handle.getColumnType());
        }
        this.columnTypes = types.build();
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new KairosdbRecordCursor(client, split, columnHandles);
    }
}
