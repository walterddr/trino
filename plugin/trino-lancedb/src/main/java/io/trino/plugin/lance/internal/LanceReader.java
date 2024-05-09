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
package io.trino.plugin.lance.internal;

import com.lancedb.lance.Dataset;
import io.trino.plugin.lance.LanceColumnHandle;
import io.trino.plugin.lance.LanceConfig;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LanceReader
{
    private static final BufferAllocator allocator = new RootAllocator(
            RootAllocator.configBuilder().from(RootAllocator.defaultConfig()).maxAllocation(4 * 1024 * 1024).build());
    private static final String LOCAL_FILE_PREFIX = "file://";

    private final URI lanceDbURI;
    private final Path dbPath;

    public LanceReader(LanceConfig lanceConfig)
    {
        // TODO: use https://lancedb.github.io/lancedb/python/python/#lancedb.db.DBConnection
        lanceDbURI = lanceConfig.getLanceDbUri();
        dbPath = Path.of(lanceDbURI);
    }

    private static Schema getSchema(Path tablePath)
    {
        try (Dataset dataset = Dataset.open(tablePath.toUri().toString(), allocator)) {
            return dataset.getSchema();
        }
    }

    private static Path getTablePath(Path dbPath, String tableName)
    {
        return dbPath.resolve(tableName);
    }

    public List<SchemaTableName> listTables(ConnectorSession session, String schema)
    {
        // TODO: local fs impl here to be replaced by
        // https://lancedb.github.io/lancedb/python/python/#lancedb.db.DBConnection.table_names
        try (Stream<Path> stream = Files.list(dbPath)) {
            return stream.filter(Files::isDirectory)
                    .map(f -> new SchemaTableName(schema, f.getFileName().toString())).collect(Collectors.toList());
        }
        catch (IOException e) {
            return Collections.emptyList();
        }
    }

    public Map<String, ColumnHandle> getColumnHandle(String tableName)
    {
        Path tablePath = getTablePath(dbPath, tableName);
        Schema arrowSchema = getSchema(tablePath);
        return arrowSchema.getFields().stream().collect(Collectors.toMap(Field::getName,
                f -> new LanceColumnHandle(f.getName(), LanceColumnHandle.toTrinoType(f.getFieldType().getType()),
                        f.getFieldType())));
    }

    public Path getTablePath(SchemaTableName schemaTableName)
    {
        return getTablePath(dbPath, schemaTableName.getTableName());
    }
}
