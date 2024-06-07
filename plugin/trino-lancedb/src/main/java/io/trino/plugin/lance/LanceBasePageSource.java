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

import com.lancedb.lance.ipc.ScanOptions;
import io.trino.plugin.lance.internal.LanceArrowToPageScanner;
import io.trino.plugin.lance.internal.LanceReader;
import io.trino.plugin.lance.internal.LanceUtils;
import io.trino.plugin.lance.internal.ScannerFactory;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.VisibleForTesting;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;

public abstract class LanceBasePageSource
        implements ConnectorPageSource
{
    private static final BufferAllocator allocator = new RootAllocator(
            RootAllocator.configBuilder().from(RootAllocator.defaultConfig()).maxAllocation(Integer.MAX_VALUE).build());

    protected final int maxReadRowsRetries;
    protected final LanceTableHandle tableHandle;

    protected final AtomicLong readBytes = new AtomicLong();
    protected final AtomicLong readRows = new AtomicLong();
    protected final AtomicBoolean isFinished = new AtomicBoolean();

    protected final LanceArrowToPageScanner lanceArrowToPageScanner;
    protected final BufferAllocator bufferAllocator;
    protected final PageBuilder pageBuilder;
    protected final long limit;

    public LanceBasePageSource(LanceReader lanceReader, LanceTableHandle tableHandle, int maxReadRowsRetries)
    {
        this.maxReadRowsRetries = maxReadRowsRetries;
        this.tableHandle = tableHandle;
        List<LanceColumnHandle> columns = toColumnHandles(lanceReader, tableHandle);
        this.bufferAllocator = allocator.newChildAllocator(tableHandle.getTableName(), 1024, Long.MAX_VALUE);
        this.lanceArrowToPageScanner =
                new LanceArrowToPageScanner(bufferAllocator, tableHandle.getTablePath(), columns, getScannerFactory(), buildScannerOption(tableHandle));
        this.pageBuilder =
                new PageBuilder(columns.stream().map(LanceColumnHandle::trinoType).collect(toImmutableList()));
        this.isFinished.set(false);
        this.limit = tableHandle.getLimit().isEmpty() ? -1 : tableHandle.getLimit().getAsLong();
    }

    private static ScanOptions buildScannerOption(LanceTableHandle tableHandle)
    {
        ScanOptions.Builder builder = new ScanOptions.Builder();
        // apply filter clause
        for (String filterClause : LanceUtils.getFilterClause(tableHandle.getConstraints())) {
            builder.filter(filterClause);
        }
        // apply column projection if exist.
        if (!tableHandle.getProjections().isEmpty()) {
            builder.columns(LanceUtils.getProjections(tableHandle.getProjections()));
        }
        return builder.build();
    }

    public abstract ScannerFactory getScannerFactory();

    @VisibleForTesting
    public static List<LanceColumnHandle> toColumnHandles(LanceReader lanceReader, LanceTableHandle tableHandle)
    {
        return lanceReader.getColumnHandle(tableHandle.getTableName()).entrySet().stream()
                .filter(e -> tableHandle.containsProjection(e.getKey()))
                .map(e -> (LanceColumnHandle) e.getValue()).collect(Collectors.toList());
    }

    @Override
    public long getCompletedBytes()
    {
        return readBytes.get();
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0L;
    }

    @Override
    public boolean isFinished()
    {
        return isFinished.get();
    }

    @Override
    public Page getNextPage()
    {
        checkState(pageBuilder.isEmpty(), "PageBuilder is not empty at the beginning of a new page");
        if (!lanceArrowToPageScanner.read() || (limit > 0 && readRows.get() >= limit)) {
            isFinished.set(true);
            return null;
        }
        lanceArrowToPageScanner.convert(pageBuilder);
        Page page = pageBuilder.build();
        pageBuilder.reset();
        readBytes.getAndAdd(page.getSizeInBytes());
        readRows.getAndAdd(page.getPositionCount());
        return page;
    }

    @Override
    public long getMemoryUsage()
    {
        return 0L;
    }

    @Override
    public void close()
    {
        lanceArrowToPageScanner.close();
        bufferAllocator.close();
    }
}
