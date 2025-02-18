/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.iceberg.catalog.file;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.units.Duration;
import io.trino.metastore.cache.CachingHiveMetastoreConfig;
import io.trino.plugin.hive.HideDeltaLakeTables;
import io.trino.plugin.hive.metastore.CachingHiveMetastoreModule;
import io.trino.plugin.hive.metastore.file.FileMetastoreModule;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.plugin.iceberg.catalog.hms.TrinoHiveCatalogFactory;
import io.trino.plugin.iceberg.procedure.MigrateProcedure;
import io.trino.spi.procedure.Procedure;

import java.util.concurrent.TimeUnit;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.iceberg.catalog.hms.IcebergHiveMetastoreCatalogModule.HIDE_DELTA_LAKE_TABLES_IN_ICEBERG;

public class IcebergFileMetastoreCatalogModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(new FileMetastoreModule());
        binder.bind(IcebergTableOperationsProvider.class).to(FileMetastoreTableOperationsProvider.class).in(Scopes.SINGLETON);
        binder.bind(TrinoCatalogFactory.class).to(TrinoHiveCatalogFactory.class).in(Scopes.SINGLETON);
        binder.bind(Key.get(boolean.class, HideDeltaLakeTables.class)).toInstance(HIDE_DELTA_LAKE_TABLES_IN_ICEBERG);
        install(new CachingHiveMetastoreModule());

        configBinder(binder).bindConfigDefaults(CachingHiveMetastoreConfig.class, config -> {
            // ensure caching metastore wrapper isn't created, as it's not leveraged by Iceberg
            config.setStatsCacheTtl(new Duration(0, TimeUnit.SECONDS));
        });
        Multibinder<Procedure> procedures = newSetBinder(binder, Procedure.class);
        procedures.addBinding().toProvider(MigrateProcedure.class).in(Scopes.SINGLETON);
    }
}
