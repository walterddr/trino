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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.plugin.base.projection.ApplyProjectionUtil;
import io.trino.plugin.lance.internal.LanceReader;
import io.trino.plugin.lance.internal.LanceUtils;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.base.projection.ApplyProjectionUtil.extractSupportedProjectedColumns;
import static io.trino.plugin.base.projection.ApplyProjectionUtil.replaceWithNewVariables;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class LanceMetadata
        implements ConnectorMetadata
{
    private final LanceReader lanceReader;
    private final LanceConfig lanceConfig;

    @Inject
    public LanceMetadata(LanceReader lanceReader, LanceConfig lanceConfig)
    {
        this.lanceReader = requireNonNull(lanceReader, "lanceClient is null");
        this.lanceConfig = lanceConfig;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(LanceReader.SCHEMA);
    }

    @Override
    public LanceTableHandle getTableHandle(ConnectorSession session, SchemaTableName name,
            Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        Path tablePath = lanceReader.getTablePath(session, name);
        if (tablePath != null) {
            return new LanceTableHandle(name.getSchemaName(), name.getTableName(), tablePath.toUri().toString());
        }
        else {
            return null;
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        LanceTableHandle lanceTableHandle = (LanceTableHandle) table;
        try {
            List<ColumnMetadata> columnsMetadata = lanceReader.getColumnsMetadata(((LanceTableHandle) table).getTableName());
            SchemaTableName schemaTableName =
                    new SchemaTableName(lanceTableHandle.getSchemaName(), lanceTableHandle.getTableName());
            return new ConnectorTableMetadata(schemaTableName, columnsMetadata);
        }
        catch (Exception e) {
            return null;
        }
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaNameOrNull)
    {
        return lanceReader.listTables(session, schemaNameOrNull.orElse(LanceReader.SCHEMA));
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        LanceTableHandle lanceTableHandle = (LanceTableHandle) tableHandle;
        try {
            return lanceReader.getColumnHandle(lanceTableHandle.getTableName());
        }
        catch (Exception e) {
            throw new TableNotFoundException(new SchemaTableName(lanceTableHandle.getSchemaName(), lanceTableHandle.getTableName()));
        }
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session,
            SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : lanceReader.listTables(session, prefix.toString())) {
            ConnectorTableMetadata tableMetadata =
                    new ConnectorTableMetadata(tableName, lanceReader.getColumnsMetadata(tableName.getTableName()));
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle)
    {
        return ((LanceColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(ConnectorSession session,
            ConnectorTableHandle table, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
    {
        Map<ConnectorExpression, ApplyProjectionUtil.ProjectedColumnRepresentation> columnProjections = projections.stream()
                .collect(toImmutableMap(identity(), ApplyProjectionUtil::createProjectedColumnRepresentation));
        if (columnProjections.values().stream().allMatch(ApplyProjectionUtil.ProjectedColumnRepresentation::isVariable)) {
            Set<LanceColumnHandle> projectedColumns = assignments.values().stream()
                    .map(LanceColumnHandle.class::cast)
                    .collect(toImmutableSet());
            LanceTableHandle oldHandle = (LanceTableHandle) table;
            if (oldHandle.getProjections().equals(projectedColumns)) {
                return Optional.empty();
            }
            List<Assignment> assignmentsList = assignments.entrySet().stream()
                    .map(assignment -> new Assignment(
                            assignment.getKey(),
                            assignment.getValue(),
                            ((LanceColumnHandle) assignment.getValue()).trinoType()))
                    .collect(toImmutableList());
            LanceTableHandle newHandle = new LanceTableHandle(
                    oldHandle.getSchemaName(),
                    oldHandle.getTableName(),
                    oldHandle.getTablePath(),
                    projectedColumns,
                    oldHandle.getConstraints(),
                    oldHandle.getLimit());
            return Optional.of(new ProjectionApplicationResult<>(
                    newHandle,
                    projections,
                    assignmentsList,
                    false));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session,
            ConnectorTableHandle table, long limit)
    {
        LanceTableHandle oldHandle = (LanceTableHandle) table;
        if (oldHandle.getLimit().isPresent() && oldHandle.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        } else {
            LanceTableHandle newHandle = new LanceTableHandle(
                    oldHandle.getSchemaName(),
                    oldHandle.getTableName(),
                    oldHandle.getTablePath(),
                    oldHandle.getProjections(),
                    oldHandle.getConstraints(),
                    OptionalLong.of(limit));
            // currently limit is applied after scan without x-split guarantee
            return Optional.of(new LimitApplicationResult<>(newHandle, false, false));
        }
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session,
            ConnectorTableHandle table, Constraint constraint)
    {
        LanceTableHandle oldHandle = (LanceTableHandle) table;
        TupleDomain<ColumnHandle> oldDomain = oldHandle.getConstraints();

        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary());
        TupleDomain<ColumnHandle> remainingFilter;
        if (newDomain.isNone()) {
            remainingFilter = TupleDomain.all();
        }
        else {
            Map<ColumnHandle, Domain> domains = newDomain.getDomains().orElseThrow();

            Map<ColumnHandle, Domain> supported = new HashMap<>();
            Map<ColumnHandle, Domain> unsupported = new HashMap<>();
            for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
                if (LanceUtils.isSupportedFilter((LanceColumnHandle) entry.getKey(), entry.getValue())) {
                    unsupported.put(entry.getKey(), entry.getValue());
                }
                else {
                    supported.put(entry.getKey(), entry.getValue());
                }
            }
            newDomain = TupleDomain.withColumnDomains(supported);
            remainingFilter = TupleDomain.withColumnDomains(unsupported);
        }

        if (oldDomain.equals(newDomain)) {
            return Optional.empty();
        }

        LanceTableHandle newHandle = new LanceTableHandle(
                oldHandle.getSchemaName(),
                oldHandle.getTableName(),
                oldHandle.getTablePath(),
                oldHandle.getProjections(),
                newDomain,
                oldHandle.getLimit());
        return Optional.of(new ConstraintApplicationResult<>(newHandle, remainingFilter, constraint.getExpression(), false));
    }

    @VisibleForTesting
    public LanceConfig getLanceConfig()
    {
        return lanceConfig;
    }

    @VisibleForTesting
    public LanceReader getLanceReader()
    {
        return lanceReader;
    }
}
