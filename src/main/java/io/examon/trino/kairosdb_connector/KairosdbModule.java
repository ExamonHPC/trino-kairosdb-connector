package io.examon.trino.kairosdb_connector;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class KairosdbModule
        implements Module
{
    private final String catalogName;

    public KairosdbModule(String catalogName)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(KairosdbConnectorId.class).toInstance(new KairosdbConnectorId(catalogName));
        binder.bind(KairosdbConnector.class).in(Scopes.SINGLETON);
        binder.bind(KairosdbMetadata.class).in(Scopes.SINGLETON);
        binder.bind(KairosdbClient.class).in(Scopes.SINGLETON);
        binder.bind(KairosdbSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(KairosdbRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(KairosdbSessionProperties.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(KairosdbConfig.class);
    }
}
