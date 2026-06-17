package io.examon.trino.kairosdb_connector;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.examon.trino.kairosdb_connector.KairosdbResponses.QueryDatapointsResponse.DataResult;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static io.examon.trino.kairosdb_connector.KairosdbErrorCode.KAIROSDB_PARSE_ERROR;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Lazy cursor that walks every {@code [timestamp, value]} pair returned by
 * KairosDB for the split's metric and time window, flattened across all tag
 * combinations.
 *
 * <p>Columns are projected by {@link KairosdbColumnHandle} ordinal position
 * (not by name) so the same cursor can serve any column order Trino asks
 * for.  The reserved names {@code timestamp} and {@code value} are detected
 * from the column name; everything else is treated as a tag and read from
 * the current group's tag map.
 */
public class KairosdbRecordCursor
        implements RecordCursor
{
    private static final Logger log = Logger.get(KairosdbRecordCursor.class);

    private static final String TIMESTAMP_COLUMN = "timestamp";
    private static final String VALUE_COLUMN = "value";

    private final KairosdbClient client;
    private final KairosdbSplit split;
    private final List<KairosdbColumnHandle> columns;
    private final List<Type> columnTypes;
    private final TimeZoneKey sessionZone;

    private final long readStartNanos = System.nanoTime();
    private long completedBytes;

    private boolean fetched;
    private Iterator<DataResult> resultIterator;
    private DataResult currentResult;
    private Iterator<List<Object>> currentPointIterator;
    private long currentTimestampMillis;
    private Object currentValue;

    public KairosdbRecordCursor(KairosdbClient client, KairosdbSplit split, List<KairosdbColumnHandle> columns, TimeZoneKey sessionZone)
    {
        this.client = requireNonNull(client, "client is null");
        this.split = requireNonNull(split, "split is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.sessionZone = requireNonNull(sessionZone, "sessionZone is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (KairosdbColumnHandle column : columns) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return System.nanoTime() - readStartNanos;
    }

    @Override
    public Type getType(int field)
    {
        return columnTypes.get(field);
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!fetched) {
            fetchOnce();
        }
        while (true) {
            if (currentPointIterator != null && currentPointIterator.hasNext()) {
                List<Object> point = currentPointIterator.next();
                if (point.size() < 2) {
                    continue;
                }
                currentTimestampMillis = ((Number) point.get(0)).longValue();
                currentValue = point.get(1);
                // Lower-bound estimate: 16B for timestamp+value plus a few bytes per tag.
                completedBytes += 16 + (currentResult.tags == null ? 0 : 8L * currentResult.tags.size());
                return true;
            }
            if (resultIterator != null && resultIterator.hasNext()) {
                currentResult = resultIterator.next();
                currentPointIterator = currentResult.values == null
                        ? ImmutableList.<List<Object>>of().iterator()
                        : currentResult.values.iterator();
                continue;
            }
            return false;
        }
    }

    private void fetchOnce()
    {
        fetched = true;
        List<DataResult> results = client.queryDatapoints(
                split.getTableName(),
                split.getStartMillis(),
                split.getEndMillis(),
                split.getTagFilters(),
                split.getLimit(),
                split.getAggregators());
        this.resultIterator = results.iterator();
    }

    @Override
    public boolean getBoolean(int field)
    {
        throw new UnsupportedOperationException("BOOLEAN columns are not supported by the KairosDB connector");
    }

    @Override
    public long getLong(int field)
    {
        KairosdbColumnHandle column = columns.get(field);
        if (isTimestampColumn(column)) {
            return packTimestamp(column.getColumnType(), currentTimestampMillis, sessionZone);
        }
        if (currentValue instanceof Number n) {
            return n.longValue();
        }
        try {
            return Long.parseLong(String.valueOf(currentValue));
        }
        catch (NumberFormatException e) {
            throw new TrinoException(KAIROSDB_PARSE_ERROR,
                    format("Cannot coerce KairosDB value '%s' to BIGINT for column %s", currentValue, column.getColumnName()), e);
        }
    }

    @Override
    public double getDouble(int field)
    {
        if (currentValue instanceof Number n) {
            return n.doubleValue();
        }
        try {
            return Double.parseDouble(String.valueOf(currentValue));
        }
        catch (NumberFormatException e) {
            throw new TrinoException(KAIROSDB_PARSE_ERROR,
                    format("Cannot coerce KairosDB value '%s' to DOUBLE for column %s", currentValue, columns.get(field).getColumnName()), e);
        }
    }

    @Override
    public Slice getSlice(int field)
    {
        KairosdbColumnHandle column = columns.get(field);
        String name = column.getColumnName();
        if (VALUE_COLUMN.equalsIgnoreCase(name)) {
            return Slices.utf8Slice(String.valueOf(currentValue));
        }
        // Tag columns: look up by exact, then case-insensitive name in the current result's tag map.
        String tagValue = lookupTagValue(currentResult, name);
        return tagValue == null ? Slices.utf8Slice("") : Slices.utf8Slice(tagValue);
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException("Complex types are not produced by the KairosDB connector");
    }

    @Override
    public boolean isNull(int field)
    {
        KairosdbColumnHandle column = columns.get(field);
        String name = column.getColumnName();
        if (isTimestampColumn(column)) {
            return false;
        }
        if (VALUE_COLUMN.equalsIgnoreCase(name)) {
            return currentValue == null;
        }
        return lookupTagValue(currentResult, name) == null;
    }

    @Override
    public void close()
    {
    }

    private static boolean isTimestampColumn(KairosdbColumnHandle column)
    {
        return TIMESTAMP_COLUMN.equalsIgnoreCase(column.getColumnName());
    }

    static long packTimestamp(Type type, long millis, TimeZoneKey zone)
    {
        String name = type.getDisplayName();
        if ("bigint".equals(name)) {
            return millis;
        }
        if (name.startsWith("timestamp(") && name.contains("with time zone")) {
            // Same instant (epoch ms UTC), rendered in the session zone so the
            // SQL client localizes the display under SET TIME ZONE.
            return packDateTimeWithZone(millis, zone);
        }
        if (name.startsWith("timestamp(")) {
            // Trino timestamps without timezone are stored as microseconds.
            return millis * 1_000L;
        }
        throw new TrinoException(KAIROSDB_PARSE_ERROR, "Unsupported timestamp type: " + name);
    }

    private static String lookupTagValue(DataResult result, String columnName)
    {
        if (result == null || result.tags == null) {
            return null;
        }
        Map<String, List<String>> tags = result.tags;
        List<String> exact = tags.get(columnName);
        if (exact != null && !exact.isEmpty()) {
            return exact.get(0);
        }
        for (Map.Entry<String, List<String>> entry : tags.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(columnName) && !entry.getValue().isEmpty()) {
                return entry.getValue().get(0);
            }
        }
        return null;
    }
}
