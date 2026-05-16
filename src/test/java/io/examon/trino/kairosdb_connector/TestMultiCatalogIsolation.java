package io.examon.trino.kairosdb_connector;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Locks in per-catalog isolation: the connector classes must hold
 * per-catalog state, not static singletons.  Two catalogs pointing at
 * different KairosDB clusters must produce two independent
 * {@link KairosdbConfig} / {@link KairosdbClient} graphs; otherwise the
 * first catalog to load silently overrides the second.
 *
 * <p>The test exercises the exact bootstrap path
 * {@link KairosdbConnectorFactory} uses (minus the SPI version check and
 * the {@code TypeDeserializerModule}, which both need a ConnectorContext
 * we don't construct here).  If a future change reintroduces a
 * {@code static} catalog-config field, the URL comparison below will fail
 * because the second injector will see the first catalog's URL.
 */
final class TestMultiCatalogIsolation
{
    @Test
    void twoCatalogsHaveIndependentConfigAndClient()
    {
        KairosdbConfig configA = bootstrap("catalog-a", Map.of("kairosdb.url", "http://kairos-a:8080")).getInstance(KairosdbConfig.class);
        KairosdbConfig configB = bootstrap("catalog-b", Map.of("kairosdb.url", "http://kairos-b:8080")).getInstance(KairosdbConfig.class);

        assertThat(configA).isNotSameAs(configB);
        assertThat(configA.getKairosdbUri().toString()).isEqualTo("http://kairos-a:8080");
        assertThat(configB.getKairosdbUri().toString()).isEqualTo("http://kairos-b:8080");
    }

    @Test
    void twoCatalogsHaveIndependentClientAndMetadataAndSplitManager()
    {
        Injector a = bootstrap("catalog-a", Map.of("kairosdb.url", "http://kairos-a:8080"));
        Injector b = bootstrap("catalog-b", Map.of("kairosdb.url", "http://kairos-b:8080"));

        // Every connector-level service must come out as a distinct instance.
        // Same Guice scope (Scopes.SINGLETON) but different injector roots.
        assertThat(a.getInstance(KairosdbClient.class)).isNotSameAs(b.getInstance(KairosdbClient.class));
        assertThat(a.getInstance(KairosdbMetadata.class)).isNotSameAs(b.getInstance(KairosdbMetadata.class));
        assertThat(a.getInstance(KairosdbSplitManager.class)).isNotSameAs(b.getInstance(KairosdbSplitManager.class));
        assertThat(a.getInstance(KairosdbRecordSetProvider.class)).isNotSameAs(b.getInstance(KairosdbRecordSetProvider.class));
        assertThat(a.getInstance(KairosdbSessionProperties.class)).isNotSameAs(b.getInstance(KairosdbSessionProperties.class));

        // ConnectorId is also catalog-scoped: don't let the catalog names cross
        // over the way the old static-field code did.
        assertThat(a.getInstance(KairosdbConnectorId.class).toString()).isEqualTo("catalog-a");
        assertThat(b.getInstance(KairosdbConnectorId.class).toString()).isEqualTo("catalog-b");
    }

    @Test
    void connectorIsSingletonWithinAnInjector()
    {
        // Belt-and-suspenders: two getInstance calls on the SAME injector
        // must return the same Connector object.  This guarantees that the
        // "no singleton across catalogs" rule above is enforced at the
        // injector boundary, not by accident at the call site.
        Injector injector = bootstrap("catalog-a", Map.of("kairosdb.url", "http://kairos-a:8080"));
        assertThat(injector.getInstance(KairosdbConnector.class))
                .isSameAs(injector.getInstance(KairosdbConnector.class));
    }

    private static Injector bootstrap(String catalogName, Map<String, String> properties)
    {
        // Mirrors KairosdbConnectorFactory.create() without TypeDeserializerModule
        // (which needs a ConnectorContext we don't construct here) and without
        // checkStrictSpiVersionMatch (irrelevant in-process).
        Bootstrap app = new Bootstrap(
                new JsonModule(),
                new KairosdbModule(catalogName));
        try {
            return app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(ImmutableMap.copyOf(properties))
                    .initialize();
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to bootstrap injector for " + catalogName, e);
        }
    }
}
