<?php

declare(strict_types=1);

namespace Migration\Migrators;

use Migration\Converters\TypeConverter;
use Migration\Database\ConnectionManager;
use Migration\Logger\ProgressLogger;
use PDO;
use PDOException;

class DataMigrator
{
    private ConnectionManager $connectionManager;
    private TypeConverter $typeConverter;
    private ProgressLogger $logger;
    private array $config;
    private bool $dryRun;
    private array $schemaMapping;

    public function __construct(
        ConnectionManager $connectionManager,
        TypeConverter $typeConverter,
        ProgressLogger $logger,
        array $config,
        array $schemaMapping,
        bool $dryRun = false
    ) {
        $this->connectionManager = $connectionManager;
        $this->typeConverter = $typeConverter;
        $this->logger = $logger;
        $this->config = $config;
        $this->dryRun = $dryRun;
        $this->schemaMapping = $schemaMapping;
    }

    public function migrateData(array $tablesInclude = [], array $tablesExclude = [], bool $resume = false, ?string $sourceSchema = null, ?string $afterDate = null, ?string $beforeDate = null, ?string $dateColumn = null): void
    {
        $this->logger->info('Starting data migration...');
        
        if ($sourceSchema !== null) {
            $this->logger->info("Migrating from PostgreSQL schema: {$sourceSchema}");
        }
        
        if ($dateColumn !== null) {
            $filters = [];
            if ($afterDate !== null) {
                $filters[] = "{$dateColumn} >= {$afterDate}";
            }
            if ($beforeDate !== null) {
                $filters[] = "{$dateColumn} < {$beforeDate}";
            }
            if (!empty($filters)) {
                $this->logger->info("Date filter enabled: Only migrating rows where " . implode(' AND ', $filters));
            }
        }
        
        $sourcePdo = $this->connectionManager->getSourceConnection();
        $targetPdo = $this->connectionManager->getTargetConnection();

        // Disable foreign key checks during data migration
        $this->logger->info('Disabling foreign key checks for data migration...');
        try {
            $targetPdo->exec('SET FOREIGN_KEY_CHECKS = 0');
        } catch (\Exception $e) {
            $this->logger->warning('Could not disable foreign key checks: ' . $e->getMessage());
        }

        // Get tables to migrate with schema info
        $tablesWithSchemas = $this->getTables($sourcePdo, $tablesInclude, $tablesExclude, $sourceSchema);
        $this->logger->info("Found " . count($tablesWithSchemas) . " tables to migrate");

        // Sort tables by size (estimate) - migrate smaller tables first
        $tablesWithSchemas = $this->sortTablesBySize($sourcePdo, $tablesWithSchemas);

        try {
            foreach ($tablesWithSchemas as $tableInfo) {
                $table = $tableInfo['table'];
                $schema = $tableInfo['schema'];
                try {
                    $this->migrateTable($sourcePdo, $targetPdo, $table, $schema, $resume, $afterDate, $beforeDate, $dateColumn);
                } catch (\Exception $e) {
                    $this->logger->error("Failed to migrate table {$schema}.{$table}: " . $e->getMessage());
                    throw $e;
                }
            }
        } finally {
            // Always re-enable foreign key checks, even if migration fails
            $this->logger->info('Re-enabling foreign key checks...');
            try {
                $targetPdo->exec('SET FOREIGN_KEY_CHECKS = 1');
            } catch (\Exception $e) {
                $this->logger->warning('Could not re-enable foreign key checks: ' . $e->getMessage());
            }
        }

        $this->logger->success('Data migration completed');
    }

    private function migrateTable(PDO $sourcePdo, PDO $targetPdo, string $table, string $schema, bool $resume, ?string $afterDate = null, ?string $beforeDate = null, ?string $dateColumn = null): void
    {
        $this->logger->info("Migrating data for table: {$schema}.{$table}");

        // Check for checkpoint
        $checkpoint = null;
        if ($resume) {
            $checkpoint = $this->logger->loadCheckpoint($table);
            if ($checkpoint) {
                $this->logger->info("Resuming from checkpoint: offset={$checkpoint['last_offset']}");
            }
        }

        // Get table info (with date filter if provided)
        $tableInfo = $this->getTableInfo($sourcePdo, $table, $schema, $afterDate, $beforeDate, $dateColumn);
        $totalRows = $tableInfo['row_count'];
        $columns = $tableInfo['columns'];
        $primaryKey = $this->schemaMapping[$table]['columns'] ?? [];

        if ($totalRows === 0) {
            $this->logger->info("Table {$schema}.{$table} is empty, skipping");
            return;
        }

        // Determine chunk size (memory-aware)
        $chunkSize = $this->determineChunkSize($tableInfo['estimated_size_mb']);
        
        // Log memory info for debugging
        $memoryLimitMB = round($this->getMemoryLimitBytes() / 1024 / 1024, 0);
        $memoryLimitBytes = $this->getMemoryLimitBytes();
        $availableMemoryMB = round(($memoryLimitBytes * 0.2) / 1024 / 1024, 0);
        $this->logger->info("Table {$schema}.{$table}: {$totalRows} rows, chunk size: {$chunkSize} (memory limit: {$memoryLimitMB}MB, available: {$availableMemoryMB}MB)");

        // Calculate starting offset
        $startOffset = $checkpoint ? $checkpoint['last_offset'] : 0;
        $processedRows = $startOffset;

        // Get primary key column for cursor-based pagination (more efficient than OFFSET)
        $pkColumn = $this->getPrimaryKeyColumn($table);
        $useCursorPagination = $pkColumn !== null;

        while ($processedRows < $totalRows) {
            $chunkData = $this->fetchChunk(
                $sourcePdo,
                $table,
                $schema,
                $columns,
                $chunkSize,
                $startOffset,
                $pkColumn,
                $useCursorPagination,
                $afterDate,
                $beforeDate,
                $dateColumn
            );

            if (empty($chunkData)) {
                break;
            }

            if (!$this->dryRun) {
                $this->insertChunk($targetPdo, $table, $columns, $chunkData);
            }

            $rowsInChunk = count($chunkData);
            $processedRows += $rowsInChunk;
            $percentage = ($processedRows / $totalRows) * 100;
            
            $this->logger->progress($table, $processedRows, $totalRows, $percentage);
            
            // Get last PK value for cursor pagination BEFORE freeing memory
            $lastPkValue = null;
            if ($useCursorPagination && !empty($chunkData) && $pkColumn !== null) {
                $lastRow = end($chunkData);
                if ($lastRow !== false && isset($lastRow[$pkColumn])) {
                    $lastPkValue = $lastRow[$pkColumn];
                }
            }
            
            // Free memory immediately after processing chunk
            unset($chunkData);
            
            // Force garbage collection for low memory systems
            $memoryLimit = $this->getMemoryLimitBytes();
            if ($memoryLimit <= 134217728) { // 128MB or less
                // Collect garbage after every chunk for low memory
                gc_collect_cycles();
            } elseif ($processedRows % ($chunkSize * 5) === 0) {
                // For larger memory, collect every 5 chunks
                gc_collect_cycles();
            }

            // Save checkpoint periodically
            if ($processedRows % ($chunkSize * $this->config['checkpoint_interval']) === 0) {
                $this->logger->saveCheckpoint($table, [
                    'last_offset' => $processedRows,
                    'total_rows' => $totalRows,
                    'chunk_size' => $chunkSize,
                ]);
            }

            // Update offset for next iteration
            if ($useCursorPagination && $lastPkValue !== null) {
                $startOffset = $lastPkValue;
            } else {
                $startOffset = $processedRows;
            }
        }

        // Clear checkpoint on completion
        $this->logger->clearCheckpoint($table);
        
        // Print newline after progress bar is complete
        echo "\n";
        
        $this->logger->success("Completed migration for table: {$table}");
    }

    private function getTables(PDO $pdo, array $include = [], array $exclude = [], ?string $sourceSchema = null): array
    {
        // Use PostgreSQL-specific query that's more reliable
        $sql = "
            SELECT schemaname || '.' || tablename AS full_name, tablename
            FROM pg_tables
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
            AND schemaname NOT LIKE 'pg_temp%'
            AND schemaname NOT LIKE 'pg_toast_temp%'
        ";
        
        // Filter by specific schema if provided - use single quotes for string literal
        if ($sourceSchema !== null) {
            // Use single quotes for string literal, escape single quotes in schema name
            $escapedSchema = str_replace("'", "''", $sourceSchema);
            $sql .= " AND schemaname = '{$escapedSchema}'";
        }
        
        $sql .= " ORDER BY schemaname, tablename";

        $stmt = $pdo->query($sql);
        $allRows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $tables = [];
        foreach ($allRows as $row) {
            $fullName = $row['full_name'] ?? $row['tablename'];
            $tableName = $row['tablename'];
            $schemaName = explode('.', $fullName)[0] ?? 'public';
            
            if (!empty($include) && !in_array($tableName, $include)) {
                continue;
            }
            if (!empty($exclude) && in_array($tableName, $exclude)) {
                continue;
            }
            $tables[] = ['table' => $tableName, 'schema' => $schemaName];
        }

        // Fallback to information_schema if pg_tables returns nothing
        if (empty($tables)) {
            $sql = "
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                AND table_type = 'BASE TABLE'
            ";
            
            if ($sourceSchema !== null) {
                // Use single quotes for string literal, escape single quotes in schema name
                $escapedSchema = str_replace("'", "''", $sourceSchema);
                $sql .= " AND table_schema = '{$escapedSchema}'";
            }
            
            $sql .= " ORDER BY table_schema, table_name";

            $stmt = $pdo->query($sql);
            $allRows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            
            foreach ($allRows as $row) {
                $schemaName = $row['table_schema'];
                $tableName = $row['table_name'];
                
                if (!empty($include) && !in_array($tableName, $include)) {
                    continue;
                }
                if (!empty($exclude) && in_array($tableName, $exclude)) {
                    continue;
                }
                $tables[] = ['table' => $tableName, 'schema' => $schemaName];
            }
        }

        return $tables;
    }

    private function sortTablesBySize(PDO $pdo, array $tablesWithSchemas): array
    {
        $sizes = [];
        foreach ($tablesWithSchemas as $tableInfo) {
            $table = $tableInfo['table'];
            $schema = $tableInfo['schema'];
            try {
                $info = $this->getTableInfo($pdo, $table, $schema);
                $sizes[] = ['table' => $table, 'schema' => $schema, 'size' => $info['estimated_size_mb']];
            } catch (\Exception $e) {
                $sizes[] = ['table' => $table, 'schema' => $schema, 'size' => 0];
            }
        }

        usort($sizes, fn($a, $b) => $a['size'] <=> $b['size']);
        return array_map(fn($item) => ['table' => $item['table'], 'schema' => $item['schema']], $sizes);
    }

    private function getTableInfo(PDO $pdo, string $table, string $schema = 'public', ?string $afterDate = null, ?string $beforeDate = null, ?string $dateColumn = null): array
    {
        // Get row count (PostgreSQL - use schema-qualified name with proper quoting)
        $quotedSchema = $this->quotePostgresIdentifier($schema);
        $quotedTable = $this->quotePostgresIdentifier($table);
        $countSql = "SELECT COUNT(*) FROM {$quotedSchema}.{$quotedTable}";
        
        // Add date filter if provided
        if ($dateColumn !== null) {
            $conditions = [];
            if ($afterDate !== null) {
                $quotedDateColumn = $this->quotePostgresIdentifier($dateColumn);
                $escapedDate = str_replace("'", "''", $afterDate);
                $conditions[] = "{$quotedDateColumn} >= '{$escapedDate}'";
            }
            if ($beforeDate !== null) {
                $quotedDateColumn = $this->quotePostgresIdentifier($dateColumn);
                $escapedDate = str_replace("'", "''", $beforeDate);
                $conditions[] = "{$quotedDateColumn} < '{$escapedDate}'";
            }
            if (!empty($conditions)) {
                $countSql .= " WHERE " . implode(' AND ', $conditions);
            }
        }
        
        $rowCount = (int) $pdo->query($countSql)->fetchColumn();

        // Get estimated size - use format() function to properly quote identifiers
        $escapedSchema = str_replace("'", "''", $schema);
        $escapedTable = str_replace("'", "''", $table);
        
        $sizeMb = 0;
        try {
            // Method 1: Use format() with regclass cast
            $sizeSql = "
                SELECT 
                    pg_total_relation_size(format('%I.%I', :schema, :table)::regclass)::bigint / 1024 / 1024 AS size_mb
            ";
            $stmt = $pdo->prepare($sizeSql);
            $stmt->execute(['schema' => $schema, 'table' => $table]);
            $sizeMb = (int) ($stmt->fetchColumn() ?: 0);
        } catch (\Exception $e) {
            try {
                // Method 2: Use quoted identifiers directly in a text cast
                $quotedSchema = $this->quotePostgresIdentifier($schema);
                $quotedTable = $this->quotePostgresIdentifier($table);
                $qualifiedName = "{$quotedSchema}.{$quotedTable}";
                $sizeSql = "
                    SELECT 
                        pg_total_relation_size({$qualifiedName}::regclass)::bigint / 1024 / 1024 AS size_mb
                ";
                $sizeMb = (int) ($pdo->query($sizeSql)->fetchColumn() ?: 0);
            } catch (\Exception $e2) {
                try {
                    // Method 3: Query pg_tables directly (case-sensitive comparison)
                    $sizeSql = "
                        SELECT 
                            pg_total_relation_size(quote_ident(schemaname)||'.'||quote_ident(tablename))::bigint / 1024 / 1024 AS size_mb
                        FROM pg_tables
                        WHERE schemaname = '{$escapedSchema}' AND tablename = '{$escapedTable}'
                    ";
                    $result = $pdo->query($sizeSql);
                    if ($result) {
                        $sizeMb = (int) ($result->fetchColumn() ?: 0);
                    }
                } catch (\Exception $e3) {
                    // If all methods fail, default to 0 - size is just for optimization
                    // Don't log warning for every table to avoid log spam
                    $sizeMb = 0;
                }
            }
        }

        // Get columns - use single quotes for string literals
        $columnsSql = "
            SELECT column_name, data_type, udt_name
            FROM information_schema.columns
            WHERE table_schema = '{$escapedSchema}' AND table_name = '{$escapedTable}'
            ORDER BY ordinal_position
        ";
        $columns = $pdo->query($columnsSql)->fetchAll(PDO::FETCH_ASSOC);

        return [
            'row_count' => $rowCount,
            'estimated_size_mb' => $sizeMb,
            'columns' => $columns,
        ];
    }

    private function getPrimaryKeyColumn(string $table): ?string
    {
        if (!isset($this->schemaMapping[$table])) {
            return null;
        }

        $columns = $this->schemaMapping[$table]['columns'] ?? [];
        foreach ($columns as $column) {
            // Check if this column is part of primary key
            // This is a simplified check - in real implementation, check against primary_key definition
            if (isset($column['column_name'])) {
                // For now, return first column as potential PK
                // In production, should check actual PK definition
                return $column['column_name'];
            }
        }

        return null;
    }

    private function determineChunkSize(int $sizeMb): int
    {
        // Get PHP memory limit and calculate safe chunk size
        $memoryLimit = $this->getMemoryLimitBytes();
        
        // Very conservative: use only 20% of memory limit for chunk data
        // This leaves room for: PHP overhead, SQL strings, converted values, batch processing, PDO buffers
        $availableMemory = $memoryLimit * 0.2;
        
        // Very conservative estimate: 4KB per row
        // Accounts for: raw row data from PostgreSQL, converted values, SQL string building, 
        // array overhead, PDO statement buffers, batch processing overhead
        $estimatedBytesPerRow = 4096;
        
        // Calculate max rows that can fit in available memory
        $maxRowsByMemory = (int) ($availableMemory / $estimatedBytesPerRow);
        
        // Get configured chunk sizes
        $configuredChunkSize = $this->config['chunk_size'] ?? 10000;
        $configuredLargeChunkSize = $this->config['large_table_chunk_size'] ?? 50000;
        $largeTableThreshold = $this->config['large_table_threshold_mb'] ?? 1000;
        
        // Determine base chunk size based on table size
        $baseChunkSize = ($sizeMb > $largeTableThreshold) 
            ? $configuredLargeChunkSize 
            : $configuredChunkSize;
        
        // ALWAYS use the smaller of: configured size or memory-safe size
        // Memory safety takes absolute priority
        $safeChunkSize = min($baseChunkSize, $maxRowsByMemory);
        
        // For very low memory (150MB or less), be extra conservative
        if ($memoryLimit <= 157286400) { // 150MB (slightly above 128MB to catch 128MB)
            // Cap at 2000 rows for low memory systems to be very safe
            $safeChunkSize = min($safeChunkSize, 2000);
        }
        
        // Final safety check: NEVER exceed memory-based limit, this is the hard cap
        $finalChunkSize = min($safeChunkSize, $maxRowsByMemory);
        
        // Ensure minimum chunk size of 100 rows, but never exceed memory limit
        return max(100, min($finalChunkSize, $maxRowsByMemory));
    }

    /**
     * Get PHP memory limit in bytes
     */
    private function getMemoryLimitBytes(): int
    {
        $memoryLimit = ini_get('memory_limit');
        
        if ($memoryLimit === '-1') {
            // Unlimited memory - use a reasonable default (512MB)
            return 512 * 1024 * 1024;
        }
        
        // Parse memory limit string (e.g., "128M", "2G", "134217728")
        $memoryLimit = trim($memoryLimit);
        $lastChar = strtolower(substr($memoryLimit, -1));
        $value = (int) $memoryLimit;
        
        switch ($lastChar) {
            case 'g':
                return $value * 1024 * 1024 * 1024;
            case 'm':
                return $value * 1024 * 1024;
            case 'k':
                return $value * 1024;
            default:
                // Assume bytes if no suffix
                return $value;
        }
    }

    private function fetchChunk(
        PDO $pdo,
        string $table,
        string $schema,
        array $columns,
        int $chunkSize,
        mixed $offset,
        ?string $pkColumn,
        bool $useCursorPagination,
        ?string $afterDate = null,
        ?string $beforeDate = null,
        ?string $dateColumn = null
    ): array {
        $columnNames = array_column($columns, 'column_name');
        // PostgreSQL uses double quotes for identifiers - preserve case
        $quotedColumns = array_map([$this, 'quotePostgresIdentifier'], $columnNames);
        $columnsList = implode(', ', $quotedColumns);
        $quotedSchema = $this->quotePostgresIdentifier($schema);
        $quotedTable = $this->quotePostgresIdentifier($table);
        $qualifiedTable = "{$quotedSchema}.{$quotedTable}";

        // Build date filter WHERE clause if provided
        $dateFilter = '';
        if ($dateColumn !== null) {
            $conditions = [];
            if ($afterDate !== null) {
                $quotedDateColumn = $this->quotePostgresIdentifier($dateColumn);
                $escapedDate = str_replace("'", "''", $afterDate);
                $conditions[] = "{$quotedDateColumn} >= '{$escapedDate}'";
            }
            if ($beforeDate !== null) {
                $quotedDateColumn = $this->quotePostgresIdentifier($dateColumn);
                $escapedDate = str_replace("'", "''", $beforeDate);
                $conditions[] = "{$quotedDateColumn} < '{$escapedDate}'";
            }
            if (!empty($conditions)) {
                $dateFilter = " WHERE " . implode(' AND ', $conditions);
            }
        }

        if ($useCursorPagination && $pkColumn !== null) {
            // Cursor-based pagination (more efficient for large tables)
            $quotedPk = $this->quotePostgresIdentifier($pkColumn);
            $whereClause = $dateFilter ? $dateFilter . " AND {$quotedPk} > :offset" : " WHERE {$quotedPk} > :offset";
            $sql = "SELECT {$columnsList} FROM {$qualifiedTable}{$whereClause} ORDER BY {$quotedPk} LIMIT :limit";
            $stmt = $pdo->prepare($sql);
            $stmt->bindValue(':offset', $offset, is_int($offset) ? PDO::PARAM_INT : PDO::PARAM_STR);
            $stmt->bindValue(':limit', $chunkSize, PDO::PARAM_INT);
        } else {
            // OFFSET-based pagination
            $orderByColumn = $quotedColumns[0];
            // If date filter exists, order by date column first, then by first column
            if ($dateFilter && $dateColumn !== null) {
                $quotedDateColumn = $this->quotePostgresIdentifier($dateColumn);
                $orderByColumn = "{$quotedDateColumn}, {$orderByColumn}";
            }
            $sql = "SELECT {$columnsList} FROM {$qualifiedTable}{$dateFilter} ORDER BY {$orderByColumn} LIMIT :limit OFFSET :offset";
            $stmt = $pdo->prepare($sql);
            $stmt->bindValue(':limit', $chunkSize, PDO::PARAM_INT);
            $stmt->bindValue(':offset', $offset, PDO::PARAM_INT);
        }

        $stmt->execute();
        return $stmt->fetchAll(PDO::FETCH_ASSOC);
    }

    private function insertChunk(PDO $pdo, string $table, array $columnDefs, array $data): void
    {
        if (empty($data)) {
            return;
        }

        $columnNames = array_column($columnDefs, 'column_name');
        $quotedColumns = array_map([$this, 'quoteIdentifier'], $columnNames);
        $columnsList = implode(', ', $quotedColumns);
        $tableName = $this->quoteIdentifier($table);

        // For large chunks, split into smaller batches to avoid memory issues
        // Calculate safe batch size based on available memory
        $memoryLimit = $this->getMemoryLimitBytes();
        $availableMemory = $memoryLimit * 0.3; // Use 30% for each batch (conservative)
        
        // Estimate: each row in batch needs ~2KB (row data + SQL string + overhead)
        $estimatedBytesPerRow = 2048;
        $maxBatchSize = max(100, min(1000, (int) ($availableMemory / $estimatedBytesPerRow)));
        
        $batches = array_chunk($data, $maxBatchSize);

        foreach ($batches as $batch) {
            // Build placeholders for this batch
            $placeholders = '(' . implode(', ', array_fill(0, count($columnNames), '?')) . ')';
            $allPlaceholders = implode(', ', array_fill(0, count($batch), $placeholders));

            $sql = "INSERT INTO {$tableName} ({$columnsList}) VALUES {$allPlaceholders}";

            // Flatten data and convert values for this batch
            $values = [];
            foreach ($batch as $row) {
                foreach ($columnDefs as $colDef) {
                    $colName = $colDef['column_name'];
                    $pgType = $colDef['data_type'] ?? $colDef['udt_name'] ?? '';
                    $value = $row[$colName] ?? null;
                    
                    // Convert value if needed
                    $convertedValue = $this->typeConverter->convertValue($value, $pgType);
                    $values[] = $convertedValue;
                }
            }

            try {
                $stmt = $pdo->prepare($sql);
                $stmt->execute($values);
                // Free memory
                unset($stmt, $values, $sql);
            } catch (PDOException $e) {
                // If batch insert fails, try row by row for better error reporting
                $this->insertRowsIndividually($pdo, $table, $columnDefs, $batch, $e);
            }
        }
        
        // Free memory
        unset($batches, $data);
    }

    private function insertRowsIndividually(PDO $pdo, string $table, array $columnDefs, array $data, PDOException $originalError): void
    {
        $columnNames = array_column($columnDefs, 'column_name');
        $quotedColumns = array_map([$this, 'quoteIdentifier'], $columnNames);
        $columnsList = implode(', ', $quotedColumns);
        $tableName = $this->quoteIdentifier($table);
        $placeholders = '(' . implode(', ', array_fill(0, count($columnNames), '?')) . ')';

        $sql = "INSERT INTO {$tableName} ({$columnsList}) VALUES {$placeholders}";
        $stmt = $pdo->prepare($sql);

        $errors = [];
        $skippedRows = 0;
        foreach ($data as $index => $row) {
            $values = [];
            foreach ($columnDefs as $colDef) {
                $colName = $colDef['column_name'];
                $pgType = $colDef['data_type'] ?? $colDef['udt_name'] ?? '';
                $value = $row[$colName] ?? null;
                $convertedValue = $this->typeConverter->convertValue($value, $pgType);
                $values[] = $convertedValue;
            }

            try {
                $stmt->execute($values);
            } catch (PDOException $e) {
                // Check if it's a datetime format error
                if (strpos($e->getMessage(), 'datetime') !== false || strpos($e->getMessage(), 'date') !== false) {
                    // Try to identify which column has the issue
                    $problemColumn = $this->findProblematicColumn($row, $columnDefs);
                    $errors[] = "Row {$index} (column: {$problemColumn}): " . $e->getMessage();
                    $skippedRows++;
                } else {
                    $errors[] = "Row {$index}: " . $e->getMessage();
                }
                
                if (count($errors) > 10) {
                    break; // Limit error reporting
                }
            }
        }

        if (!empty($errors)) {
            $skippedMsg = $skippedRows > 0 ? " ({$skippedRows} rows skipped due to invalid dates)" : "";
            $errorMsg = "Batch insert failed. First errors:\n" . implode("\n", $errors) . $skippedMsg;
            throw new \RuntimeException($errorMsg, 0, $originalError);
        }
    }

    private function findProblematicColumn(array $row, array $columnDefs): string
    {
        foreach ($columnDefs as $colDef) {
            $colName = $colDef['column_name'];
            $pgType = strtolower($colDef['data_type'] ?? $colDef['udt_name'] ?? '');
            
            // Check if this is a date/time column
            if (strpos($pgType, 'timestamp') !== false || 
                strpos($pgType, 'date') !== false || 
                strpos($pgType, 'time') !== false) {
                $value = $row[$colName] ?? null;
                if ($value !== null) {
                    $valueStr = (string) $value;
                    // Check for corrupted dates (5+ digit years)
                    if (preg_match('/^(\d{5,})/', $valueStr)) {
                        return $colName . " (value: {$valueStr})";
                    }
                }
            }
        }
        return 'unknown';
    }

    private function quoteIdentifier(string $identifier): string
    {
        return '`' . str_replace('`', '``', $identifier) . '`';
    }

    private function quotePostgresIdentifier(string $identifier): string
    {
        return '"' . str_replace('"', '""', $identifier) . '"';
    }
}
