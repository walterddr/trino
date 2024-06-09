package io.trino.plugin.lance;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.List;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;

public class TestingUtils
{
    public static final SchemaTableName TEST_TABLE_1 = new SchemaTableName("default", "test_table1");

    public static final ArrowType INT64_TYPE = new ArrowType.Int(64, true);
    public static final LanceColumnHandle COLUMN_HANDLE_B = new LanceColumnHandle("b", LanceColumnHandle.toTrinoType(INT64_TYPE), FieldType.nullable(INT64_TYPE));
    public static final LanceColumnHandle COLUMN_HANDLE_C = new LanceColumnHandle("c", LanceColumnHandle.toTrinoType(INT64_TYPE), FieldType.nullable(INT64_TYPE));
    public static final LanceColumnHandle COLUMN_HANDLE_X = new LanceColumnHandle("x", LanceColumnHandle.toTrinoType(INT64_TYPE), FieldType.nullable(INT64_TYPE));
    public static final LanceColumnHandle COLUMN_HANDLE_Y = new LanceColumnHandle("y", LanceColumnHandle.toTrinoType(INT64_TYPE), FieldType.nullable(INT64_TYPE));

    public static final List<ConnectorExpression> COLUMN_PROJECTIONS =
            ImmutableList.of(new Variable("x", BIGINT), new Variable("y", BIGINT));
    public static final Map<String, ColumnHandle> COLUMN_PROJECTION_ASSIGNMENT = ImmutableMap.of(
            "x", COLUMN_HANDLE_X, "y", COLUMN_HANDLE_Y);
}
