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
        $columns = $this->getColumns($sourcePdo, $table);
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

    private function getColumns(PDO $pdo, string $table): array
    {
        $sql = "
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = :table
            ORDER BY ordinal_position
        ";
        
        $stmt = $pdo->prepare($sql);
        $stmt->execute(['table' => $table]);
        return $stmt->fetchAll(PDO::FETCH_ASSOC);
    }

    private function quoteIdentifier(string $identifier): string
    {
        return '`' . str_replace('`', '``', $identifier) . '`';
    }
}
