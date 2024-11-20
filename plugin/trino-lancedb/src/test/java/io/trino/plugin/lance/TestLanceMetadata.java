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
package io.trino.plugin.lance;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import io.trino.plugin.lance.internal.LanceReader;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.URL;
import java.util.Optional;

import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestLanceMetadata
{
    private static final String TEST_DB_PATH = "file://" + Resources.getResource(TestLanceMetadata.class, "/example_db/").getPath();
    private static final LanceTableHandle TEST_TABLE_1_HANDLE = new LanceTableHandle("default", "test_table1",
            TEST_DB_PATH + "test_table1.lance/");
    private static final LanceTableHandle TEST_TABLE_2_HANDLE = new LanceTableHandle("default", "test_table2",
            TEST_DB_PATH + "test_table2.lance/");

    private LanceMetadata metadata;

    @BeforeEach
    public void setUp()
            throws Exception
    {
        URL lanceDbURL = Resources.getResource(LanceReader.class, "/example_db");
        assertThat(lanceDbURL)
                .describedAs("example db is null")
                .isNotNull();
        LanceConfig lanceConfig = new LanceConfig().setLanceDbUri(lanceDbURL.toString());
        LanceReader lanceReader = new LanceReader(lanceConfig);
        metadata = new LanceMetadata(lanceReader, lanceConfig);
    }

    @Test
    public void testListSchemaNames()
    {
        assertThat(metadata.listSchemaNames(SESSION)).containsExactlyElementsOf(ImmutableSet.of("default"));
    }

    @Test
    public void testGetTableHandle()
    {
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("default", "test_table1"), Optional.empty(), Optional.empty())).isEqualTo(TEST_TABLE_1_HANDLE);
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("default", "test_table2"), Optional.empty(), Optional.empty())).isEqualTo(TEST_TABLE_2_HANDLE);
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("other_schema", "test_table3"), Optional.empty(), Optional.empty())).isNull();
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "unknown"), Optional.empty(), Optional.empty())).isNull();
    }

    @Test
    public void testGetColumnHandles()
    {
        // known table

        assertThat(metadata.getColumnHandles(SESSION, TEST_TABLE_1_HANDLE)).isEqualTo(ImmutableMap.of(
                "b", TestingUtils.COLUMN_HANDLE_B,
                "c", TestingUtils.COLUMN_HANDLE_C,
                "x", TestingUtils.COLUMN_HANDLE_X,
                "y", TestingUtils.COLUMN_HANDLE_Y));

        // unknown table
        assertThatThrownBy(() -> metadata.getColumnHandles(SESSION, new LanceTableHandle("unknown", "unknown", "unknown")))
                .isInstanceOf(TableNotFoundException.class)
                .hasMessage("Table 'unknown.unknown' not found");
        assertThatThrownBy(() -> metadata.getColumnHandles(SESSION, new LanceTableHandle("example", "unknown", "unknown")))
                .isInstanceOf(TableNotFoundException.class)
                .hasMessage("Table 'example.unknown' not found");
    }

    @Test
    public void getTableMetadata()
    {
        // known table
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, TEST_TABLE_1_HANDLE);
        assertThat(tableMetadata.getTable()).isEqualTo(new SchemaTableName("default", "test_table1"));
        assertThat(tableMetadata.getColumns()).isEqualTo(ImmutableList.of(
                TestingUtils.COLUMN_HANDLE_B.getColumnMetadata(),
                TestingUtils.COLUMN_HANDLE_C.getColumnMetadata(),
                TestingUtils.COLUMN_HANDLE_X.getColumnMetadata(),
                TestingUtils.COLUMN_HANDLE_Y.getColumnMetadata()));

        // unknown tables should produce null
        assertThat(metadata.getTableMetadata(SESSION, new LanceTableHandle("unknown", "unknown", "unknown"))).isNull();
        assertThat(metadata.getTableMetadata(SESSION, new LanceTableHandle("default", "unknown", "unknown"))).isNull();
    }

    @Test
    public void testListTables()
    {
        // all schemas
        assertThat(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.empty()))).isEqualTo(ImmutableSet.of(
                new SchemaTableName("default", "test_table1"),
                new SchemaTableName("default", "test_table2"),
                new SchemaTableName("default", "test_table3"),
                new SchemaTableName("default", "test_table4")));

        // specific schema
        assertThat(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.of("default")))).isEqualTo(ImmutableSet.of(
                new SchemaTableName("default", "test_table1"),
                new SchemaTableName("default", "test_table2"),
                new SchemaTableName("default", "test_table3"),
                new SchemaTableName("default", "test_table4")));
    }
}
