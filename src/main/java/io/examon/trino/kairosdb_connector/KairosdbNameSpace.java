package io.examon.trino.kairosdb_connector;

/** Static names exposed to Trino. */
public final class KairosdbNameSpace
{
    /** KairosDB has no concept of schemas; the connector exposes a single one named {@value}. */
    public static final String SCHEMA = "kairosdb";

    private KairosdbNameSpace() {}
}
