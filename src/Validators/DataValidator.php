<?php

declare(strict_types=1);

namespace Migration\Validators;

use Migration\Database\ConnectionManager;
use Migration\Logger\ProgressLogger;
use PDO;

class DataValidator
{
    private ConnectionManager $connectionManager;
    private ProgressLogger $logger;

    public function __construct(ConnectionManager $connectionManager, ProgressLogger $logger)
    {
        $this->connectionManager = $connectionManager;
        $this->logger = $logger;
    }

    public function validateMigration(array $tablesInclude = [], array $tablesExclude = []): array
    {
        $this->logger->info('Starting data validation...');
        
        $sourcePdo = $this->connectionManager->getSourceConnection();
        $targetPdo = $this->connectionManager->getTargetConnection();

        $tables = $this->getTables($sourcePdo, $tablesInclude, $tablesExclude);
        $results = [];

        foreach ($tables as $table) {
            $this->logger->info("Validating table: {$table}");
            
            try {
                $result = $this->validateTable($sourcePdo, $targetPdo, $table);
                $results[$table] = $result;
                
                if ($result['row_count_match']) {
                    $this->logger->success("Table {$table}: Row counts match ({$result['source_rows']} rows)");
                } else {
                    $this->logger->error("Table {$table}: Row count mismatch! Source: {$result['source_rows']}, Target: {$result['target_rows']}");
                }

                if ($result['sample_match']) {
                    $this->logger->success("Table {$table}: Sample data matches");
                } else {
                    $this->logger->warning("Table {$table}: Sample data mismatch detected");
                }
            } catch (\Exception $e) {
                $this->logger->error("Failed to validate table {$table}: " . $e->getMessage());
                $results[$table] = [
                    'valid' => false,
                    'error' => $e->getMessage(),
                ];
            }
        }

        $validCount = count(array_filter($results, fn($r) => ($r['valid'] ?? false)));
        $totalCount = count($results);
        
        $this->logger->info("Validation completed: {$validCount}/{$totalCount} tables passed");

        return $results;
    }

    private function validateTable(PDO $sourcePdo, PDO $targetPdo, string $table): array
    {
        // Get row counts
        $sourceRows = $this->getRowCount($sourcePdo, $table);
        $targetRows = $this->getRowCount($targetPdo, $table);
        $rowCountMatch = $sourceRows === $targetRows;

        // Sample data validation (first 100 rows)
        $sampleMatch = true;
        if ($rowCountMatch && $sourceRows > 0) {
            $sampleMatch = $this->validateSampleData($sourcePdo, $targetPdo, $table);
        }

        return [
            'valid' => $rowCountMatch && $sampleMatch,
            'source_rows' => $sourceRows,
            'target_rows' => $targetRows,
            'row_count_match' => $rowCountMatch,
            'sample_match' => $sampleMatch,
        ];
    }

    private function getRowCount(PDO $pdo, string $table): int
    {
        $sql = "SELECT COUNT(*) FROM " . $this->quoteIdentifier($table);
        return (int) $pdo->query($sql)->fetchColumn();
    }

    private function validateSampleData(PDO $sourcePdo, PDO $targetPdo, string $table): bool
    {
        // Get column names
        $columns = $this->getColumnsHelper($sourcePdo, $table);
        if (empty($columns)) {
            return true;
        }

        $columnNames = array_column($columns, 'column_name');
        $quotedColumns = array_map([$this, 'quoteIdentifier'], $columnNames);
        $columnsList = implode(', ', $quotedColumns);
        $tableName = $this->quoteIdentifier($table);

        // Fetch sample from both databases
        $sampleSize = min(100, $this->getRowCount($sourcePdo, $table));
        
        $sourceSql = "SELECT {$columnsList} FROM {$tableName} LIMIT {$sampleSize}";
        $targetSql = "SELECT {$columnsList} FROM {$tableName} LIMIT {$sampleSize}";

        $sourceData = $sourcePdo->query($sourceSql)->fetchAll(PDO::FETCH_ASSOC);
        $targetData = $targetPdo->query($targetSql)->fetchAll(PDO::FETCH_ASSOC);

        if (count($sourceData) !== count($targetData)) {
            return false;
        }

        // Compare rows (order may differ, so compare as sets)
        $sourceHashes = array_map(fn($row) => md5(json_encode($row, SORT_KEYS)), $sourceData);
        $targetHashes = array_map(fn($row) => md5(json_encode($row, SORT_KEYS)), $targetData);

        sort($sourceHashes);
        sort($targetHashes);

        return $sourceHashes === $targetHashes;
    }

    private function getTables(PDO $pdo, array $include = [], array $exclude = []): array
    {
        // Use PostgreSQL-specific query for source, information_schema for target
        // Check if this is PostgreSQL (source) or MySQL (target) by trying pg_tables
        try {
            $sql = "
                SELECT schemaname || '.' || tablename AS full_name, tablename
                FROM pg_tables
                WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                AND schemaname NOT LIKE 'pg_temp%'
                AND schemaname NOT LIKE 'pg_toast_temp%'
                ORDER BY schemaname, tablename
            ";

            $stmt = $pdo->query($sql);
            $allRows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            $tables = [];
            foreach ($allRows as $row) {
                $tableName = $row['tablename'];
                
                if (!empty($include) && !in_array($tableName, $include)) {
                    continue;
                }
                if (!empty($exclude) && in_array($tableName, $exclude)) {
                    continue;
                }
                $tables[] = $tableName;
            }

            return $tables;
        } catch (\Exception $e) {
            // Not PostgreSQL, use information_schema
            $sql = "
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'mysql', 'sys', 'performance_schema')
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            ";

            $stmt = $pdo->query($sql);
            $allTables = $stmt->fetchAll(PDO::FETCH_COLUMN);

            $tables = [];
            foreach ($allTables as $table) {
                if (!empty($include) && !in_array($table, $include)) {
                    continue;
                }
                if (!empty($exclude) && in_array($table, $exclude)) {
                    continue;
                }
                $tables[] = $table;
            }

            return $tables;
        }
    }

    private function getColumns(PDO $pdo, string $table, ?string $sourceSchema = null): array
    {
        return $this->getColumnsHelper($pdo, $table, $sourceSchema);
    }
    
    private function getColumnsHelper(PDO $pdo, string $table, ?string $sourceSchema = null): array
    {
        try {
            // Try PostgreSQL first
            if ($sourceSchema !== null) {
                $escapedSchema = str_replace("'", "''", $sourceSchema);
                $escapedTable = str_replace("'", "''", $table);
                $sql = "
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = '{$escapedSchema}' AND table_name = '{$escapedTable}'
                    ORDER BY ordinal_position
                ";
            } else {
                $escapedTable = str_replace("'", "''", $table);
                $sql = "
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = '{$escapedTable}'
                    ORDER BY ordinal_position
                ";
            }
            
            $stmt = $pdo->query($sql);
            return $stmt->fetchAll(PDO::FETCH_ASSOC);
        } catch (\Exception $e) {
            // Not PostgreSQL, try MySQL/MariaDB
            $sql = "
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = DATABASE() AND table_name = ?
                ORDER BY ordinal_position
            ";
            $stmt = $pdo->prepare($sql);
            $stmt->execute([$table]);
            return $stmt->fetchAll(PDO::FETCH_ASSOC);
        }
    }

    /**
     * Find tables and rows that were not migrated
     * Returns detailed information about missing rows
     */
    public function findMissingRows(array $tablesInclude = [], array $tablesExclude = [], ?string $sourceSchema = null, int $limitPerTable = 100): array
    {
        $this->logger->info('Finding missing rows (rows in source but not in target)...');
        
        $sourcePdo = $this->connectionManager->getSourceConnection();
        $targetPdo = $this->connectionManager->getTargetConnection();

        $tables = $this->getTables($sourcePdo, $tablesInclude, $tablesExclude);
        $results = [];

        foreach ($tables as $table) {
            $this->logger->info("Checking table: {$table}");
            
            try {
                $sourceRows = $this->getRowCount($sourcePdo, $table);
                $targetRows = $this->getRowCount($targetPdo, $table);
                
                if ($sourceRows === $targetRows) {
                    $this->logger->info("Table {$table}: Row counts match, skipping detailed check");
                    continue;
                }
                
                $this->logger->warning("Table {$table}: Row count mismatch! Source: {$sourceRows}, Target: {$targetRows}");
                
                // Find missing rows
                $missingRows = $this->findMissingRowsInTable($sourcePdo, $targetPdo, $table, $sourceSchema, $limitPerTable);
                
                $results[$table] = [
                    'table' => $table,
                    'source_rows' => $sourceRows,
                    'target_rows' => $targetRows,
                    'missing_count' => count($missingRows),
                    'missing_rows' => $missingRows,
                ];
                
                if (!empty($missingRows)) {
                    $this->logger->error("Table {$table}: Found " . count($missingRows) . " missing row(s)");
                }
            } catch (\Exception $e) {
                $this->logger->error("Failed to check table {$table}: " . $e->getMessage());
                $results[$table] = [
                    'table' => $table,
                    'error' => $e->getMessage(),
                ];
            }
        }

        $tablesWithMissingRows = count(array_filter($results, fn($r) => !empty($r['missing_rows'] ?? [])));
        $this->logger->info("Found missing rows in {$tablesWithMissingRows} table(s)");

        return $results;
    }

    private function findMissingRowsInTable(PDO $sourcePdo, PDO $targetPdo, string $table, ?string $sourceSchema, int $limit): array
    {
        // Get primary key column
        $pkColumn = $this->getPrimaryKeyColumn($sourcePdo, $table, $sourceSchema);
        
        if ($pkColumn === null) {
            // No primary key, use all columns for comparison (slower but works)
            return $this->findMissingRowsByAllColumns($sourcePdo, $targetPdo, $table, $sourceSchema, $limit);
        }
        
        // Use primary key for efficient comparison
        return $this->findMissingRowsByPrimaryKey($sourcePdo, $targetPdo, $table, $pkColumn, $sourceSchema, $limit);
    }

    private function findMissingRowsByPrimaryKey(PDO $sourcePdo, PDO $targetPdo, string $table, string $pkColumn, ?string $sourceSchema, int $limit): array
    {
        $quotedPk = $this->quoteIdentifier($pkColumn);
        $quotedTable = $this->quoteIdentifier($table);
        
        // Build source table reference
        if ($sourceSchema !== null) {
            $sourceTableRef = $this->quotePostgresIdentifier($sourceSchema) . '.' . $this->quotePostgresIdentifier($table);
        } else {
            $sourceTableRef = $quotedTable;
        }
        
        // Find rows in source that don't exist in target
        $sql = "
            SELECT s.{$quotedPk}
            FROM {$sourceTableRef} s
            WHERE NOT EXISTS (
                SELECT 1 FROM {$quotedTable} t WHERE t.{$quotedPk} = s.{$quotedPk}
            )
            LIMIT {$limit}
        ";
        
        try {
            $stmt = $sourcePdo->query($sql);
            $missingPks = $stmt->fetchAll(PDO::FETCH_COLUMN);
            
            if (empty($missingPks)) {
                return [];
            }
            
            // Get full row data for missing primary keys
            $pkPlaceholders = implode(',', array_fill(0, count($missingPks), '?'));
            $selectSql = "SELECT * FROM {$sourceTableRef} WHERE {$quotedPk} IN ({$pkPlaceholders})";
            $stmt = $sourcePdo->prepare($selectSql);
            $stmt->execute($missingPks);
            return $stmt->fetchAll(PDO::FETCH_ASSOC);
        } catch (\Exception $e) {
            $this->logger->warning("Could not find missing rows by primary key for {$table}: " . $e->getMessage());
            return [];
        }
    }

    private function findMissingRowsByAllColumns(PDO $sourcePdo, PDO $targetPdo, string $table, ?string $sourceSchema, int $limit): array
    {
        // This is slower but works when there's no primary key
        $columns = $this->getColumns($sourcePdo, $table, $sourceSchema);
        if (empty($columns)) {
            return [];
        }
        
        $columnNames = array_column($columns, 'column_name');
        $quotedColumns = array_map([$this, 'quoteIdentifier'], $columnNames);
        $columnsList = implode(', ', $quotedColumns);
        
        // Build source table reference
        if ($sourceSchema !== null) {
            $sourceTableRef = $this->quotePostgresIdentifier($sourceSchema) . '.' . $this->quotePostgresIdentifier($table);
        } else {
            $sourceTableRef = $this->quoteIdentifier($table);
        }
        
        $targetTableRef = $this->quoteIdentifier($table);
        
        // Get sample rows from source
        $sourceSql = "SELECT {$columnsList} FROM {$sourceTableRef} LIMIT {$limit * 2}";
        $sourceRows = $sourcePdo->query($sourceSql)->fetchAll(PDO::FETCH_ASSOC);
        
        if (empty($sourceRows)) {
            return [];
        }
        
        // Check each source row against target
        $missingRows = [];
        foreach ($sourceRows as $sourceRow) {
            $whereConditions = [];
            $params = [];
            
            foreach ($columnNames as $col) {
                $quotedCol = $this->quoteIdentifier($col);
                if ($sourceRow[$col] === null) {
                    $whereConditions[] = "{$quotedCol} IS NULL";
                } else {
                    $whereConditions[] = "{$quotedCol} = ?";
                    $params[] = $sourceRow[$col];
                }
            }
            
            $whereClause = implode(' AND ', $whereConditions);
            $checkSql = "SELECT COUNT(*) FROM {$targetTableRef} WHERE {$whereClause}";
            
            $stmt = $targetPdo->prepare($checkSql);
            $stmt->execute($params);
            
            if ((int) $stmt->fetchColumn() === 0) {
                $missingRows[] = $sourceRow;
                if (count($missingRows) >= $limit) {
                    break;
                }
            }
        }
        
        return $missingRows;
    }

    private function getPrimaryKeyColumn(PDO $pdo, string $table, ?string $sourceSchema = null): ?string
    {
        try {
            // Try PostgreSQL first
            if ($sourceSchema !== null) {
                $escapedSchema = str_replace("'", "''", $sourceSchema);
                $escapedTable = str_replace("'", "''", $table);
                $sql = "
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                    JOIN pg_class c ON c.oid = i.indrelid
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE i.indisprimary = true
                    AND n.nspname = '{$escapedSchema}'
                    AND c.relname = '{$escapedTable}'
                    LIMIT 1
                ";
            } else {
                $escapedTable = str_replace("'", "''", $table);
                $sql = "
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                    JOIN pg_class c ON c.oid = i.indrelid
                    WHERE i.indisprimary = true
                    AND c.relname = '{$escapedTable}'
                    LIMIT 1
                ";
            }
            
            $stmt = $pdo->query($sql);
            $pk = $stmt->fetchColumn();
            return $pk ?: null;
        } catch (\Exception $e) {
            // Not PostgreSQL or query failed, try MySQL/MariaDB
            try {
                $sql = "
                    SELECT column_name
                    FROM information_schema.key_column_usage
                    WHERE table_schema = DATABASE()
                    AND table_name = ?
                    AND constraint_name = 'PRIMARY'
                    ORDER BY ordinal_position
                    LIMIT 1
                ";
                $stmt = $pdo->prepare($sql);
                $stmt->execute([$table]);
                $pk = $stmt->fetchColumn();
                return $pk ?: null;
            } catch (\Exception $e2) {
                return null;
            }
        }
    }

    private function getColumns(PDO $pdo, string $table, ?string $sourceSchema = null): array
    {
        try {
            // Try PostgreSQL first
            if ($sourceSchema !== null) {
                $escapedSchema = str_replace("'", "''", $sourceSchema);
                $escapedTable = str_replace("'", "''", $table);
                $sql = "
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = '{$escapedSchema}' AND table_name = '{$escapedTable}'
                    ORDER BY ordinal_position
                ";
            } else {
                $escapedTable = str_replace("'", "''", $table);
                $sql = "
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = '{$escapedTable}'
                    ORDER BY ordinal_position
                ";
            }
            
            $stmt = $pdo->query($sql);
            return $stmt->fetchAll(PDO::FETCH_ASSOC);
        } catch (\Exception $e) {
            // Not PostgreSQL, try MySQL/MariaDB
            $sql = "
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = DATABASE() AND table_name = ?
                ORDER BY ordinal_position
            ";
            $stmt = $pdo->prepare($sql);
            $stmt->execute([$table]);
            return $stmt->fetchAll(PDO::FETCH_ASSOC);
        }
    }

    private function quotePostgresIdentifier(string $identifier): string
    {
        return '"' . str_replace('"', '""', $identifier) . '"';
    }

    private function quoteIdentifier(string $identifier): string
    {
        return '`' . str_replace('`', '``', $identifier) . '`';
    }
}
