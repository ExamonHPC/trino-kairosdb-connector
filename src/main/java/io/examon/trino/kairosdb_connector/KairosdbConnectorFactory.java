package io.examon.trino.kairosdb_connector;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.trino.plugin.base.TypeDeserializerModule;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;
import static java.util.Objects.requireNonNull;

public class KairosdbConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return "kairosdb";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(requiredConfig, "requiredConfig is null");
        checkStrictSpiVersionMatch(context, this);

        try {
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new TypeDeserializerModule(context.getTypeManager()),
                    new KairosdbModule(catalogName));

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfig)
                    .initialize();

            return injector.getInstance(KairosdbConnector.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
