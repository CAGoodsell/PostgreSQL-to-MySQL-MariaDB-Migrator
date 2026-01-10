<?php

declare(strict_types=1);

namespace Migration\Migrators;

use Migration\Converters\TypeConverter;
use Migration\Database\ConnectionManager;
use Migration\Logger\ProgressLogger;
use PDO;

class SchemaMigrator
{
    private ConnectionManager $connectionManager;
    private TypeConverter $typeConverter;
    private ProgressLogger $logger;
    private bool $dryRun;

    public function __construct(
        ConnectionManager $connectionManager,
        TypeConverter $typeConverter,
        ProgressLogger $logger,
        bool $dryRun = false
    ) {
        $this->connectionManager = $connectionManager;
        $this->typeConverter = $typeConverter;
        $this->logger = $logger;
        $this->dryRun = $dryRun;
    }

    public function migrateSchema(array $tablesInclude = [], array $tablesExclude = [], ?string $sourceSchema = null): array
    {
        $this->logger->info('Starting schema migration...');
        
        if ($sourceSchema !== null) {
            $this->logger->info("Migrating from PostgreSQL schema: {$sourceSchema}");
        } else {
            $this->logger->info("Migrating from all PostgreSQL schemas");
        }
        
        $sourcePdo = $this->connectionManager->getSourceConnection();
        $targetPdo = $this->connectionManager->getTargetConnection();

        // Get all tables with their schemas
        $tablesWithSchemas = $this->getTables($sourcePdo, $tablesInclude, $tablesExclude, $sourceSchema);
        $this->logger->info("Found " . count($tablesWithSchemas) . " tables to migrate");

        $schemaMapping = [];

        foreach ($tablesWithSchemas as $tableInfo) {
            $table = $tableInfo['table'];
            $schema = $tableInfo['schema'];
            $this->logger->info("Migrating schema for table: {$schema}.{$table}");
            
            try {
                $tableSchema = $this->extractTableSchema($sourcePdo, $table, $schema);
                
                if (empty($tableSchema['columns'])) {
                    $this->logger->warning("Table {$schema}.{$table} has no columns - skipping");
                    continue;
                }
                
                $this->logger->info("Table {$schema}.{$table} has " . count($tableSchema['columns']) . " columns");
                $createStatement = $this->generateCreateTableStatement($table, $tableSchema);
                
                $schemaMapping[$table] = [
                    'schema' => $schema,
                    'columns' => $tableSchema['columns'],
                    'indexes' => $tableSchema['indexes'],
                    'foreign_keys' => $tableSchema['foreign_keys'],
                ];

                if (!$this->dryRun) {
                    $targetPdo->exec($createStatement);
                    $this->logger->success("Created table: {$table}");
                } else {
                    $this->logger->info("DRY RUN - Would create table: {$table}");
                    $this->logger->info("SQL: " . $createStatement);
                }
            } catch (\Exception $e) {
                $this->logger->error("Failed to migrate schema for {$schema}.{$table}: " . $e->getMessage());
                throw $e;
            }
        }

        $this->logger->success('Schema migration completed');
        return $schemaMapping;
    }

    public function getTables(PDO $pdo, array $include = [], array $exclude = [], ?string $sourceSchema = null): array
    {
        // First, try to get the current search_path to determine which schema(s) to use
        $searchPathSql = "SHOW search_path";
        try {
            $searchPath = $pdo->query($searchPathSql)->fetchColumn();
            $this->logger->info("PostgreSQL search_path: {$searchPath}");
        } catch (\Exception $e) {
            // If SHOW doesn't work, fall back to default
            $this->logger->info("Could not determine search_path, using default schemas");
        }

        // Use PostgreSQL-specific query that's more reliable
        // Query all schemas the user has access to, excluding system schemas
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

        // Store both schema and table name
        $tables = [];
        foreach ($allRows as $row) {
            $fullName = $row['full_name'] ?? $row['tablename'];
            $tableName = $row['tablename'];
            $schemaName = explode('.', $fullName)[0] ?? 'public';
            
            // Log for debugging
            if (count($tables) < 5) {
                $this->logger->info("Found table: {$fullName} (schema: {$schemaName})");
            }
            
            if (!empty($include) && !in_array($tableName, $include)) {
                continue;
            }
            if (!empty($exclude) && in_array($tableName, $exclude)) {
                continue;
            }
            $tables[] = ['table' => $tableName, 'schema' => $schemaName];
        }

        // If no tables found with pg_tables, try information_schema with multiple schemas
        if (empty($tables)) {
            $this->logger->info("No tables found with pg_tables, trying information_schema...");
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
                
                if (count($tables) < 5) {
                    $this->logger->info("Found table via information_schema: {$schemaName}.{$tableName}");
                }
                
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

    public function extractTableSchema(PDO $pdo, string $table, string $schema = 'public'): array
    {
        // Get columns
        $columns = $this->getColumns($pdo, $table, $schema);
        
        // Get primary key
        $primaryKey = $this->getPrimaryKey($pdo, $table, $schema);
        
        // Get indexes (excluding primary key)
        $indexes = $this->getIndexes($pdo, $table, $schema);
        
        // Get foreign keys
        $foreignKeys = $this->getForeignKeys($pdo, $table, $schema);

        return [
            'columns' => $columns,
            'primary_key' => $primaryKey,
            'indexes' => $indexes,
            'foreign_keys' => $foreignKeys,
        ];
    }

    private function getColumns(PDO $pdo, string $table, string $schema = 'public'): array
    {
        $sql = "
            SELECT 
                column_name,
                data_type,
                udt_name,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                is_nullable,
                column_default,
                ordinal_position
            FROM information_schema.columns
            WHERE table_schema = :schema
            AND table_name = :table
            ORDER BY ordinal_position
        ";

        $stmt = $pdo->prepare($sql);
        $stmt->execute(['schema' => $schema, 'table' => $table]);
        return $stmt->fetchAll(PDO::FETCH_ASSOC);
    }

    private function getPrimaryKey(PDO $pdo, string $table, string $schema = 'public'): ?array
    {
        $sql = "
            SELECT 
                kcu.column_name,
                kcu.ordinal_position
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = :schema
            AND tc.table_name = :table
            ORDER BY kcu.ordinal_position
        ";

        $stmt = $pdo->prepare($sql);
        $stmt->execute(['schema' => $schema, 'table' => $table]);
        $columns = $stmt->fetchAll(PDO::FETCH_ASSOC);

        if (empty($columns)) {
            return null;
        }

        return [
            'columns' => array_column($columns, 'column_name'),
        ];
    }

    private function getIndexes(PDO $pdo, string $table, string $schema = 'public'): array
    {
        // Query to get index information including column ordering (ASC/DESC)
        // PostgreSQL stores sort direction in pg_index.indoption array
        // Bit 0 of each indoption element: 1 = DESC, 0 = ASC
        // Use unnest with ordinality to get proper column order and position
        $sql = "
            SELECT
                i.relname AS index_name,
                a.attname AS column_name,
                ix.indisunique AS is_unique,
                am.amname AS index_type,
                key_pos.pos AS key_position,
                COALESCE(opt.opt_value, 0) AS option_value
            FROM pg_class t
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_index ix ON t.oid = ix.indrelid
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_am am ON i.relam = am.oid
            JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS key_pos(key_attnum, pos) ON true
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = key_pos.key_attnum
            LEFT JOIN LATERAL unnest(ix.indoption) WITH ORDINALITY AS opt(opt_value, opt_pos) 
                ON opt.opt_pos = key_pos.pos
            WHERE t.relkind = 'r'
            AND n.nspname = :schema
            AND t.relname = :table
            AND NOT ix.indisprimary
            ORDER BY i.relname, key_pos.pos
        ";

        $stmt = $pdo->prepare($sql);
        $stmt->execute(['schema' => $schema, 'table' => $table]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $indexes = [];
        foreach ($rows as $row) {
            $indexName = $row['index_name'];
            if (!isset($indexes[$indexName])) {
                $indexes[$indexName] = [
                    'name' => $indexName,
                    'columns' => [],
                    'unique' => $row['is_unique'] === 't',
                    'type' => $row['index_type'],
                ];
            }
            
            // Extract sort direction from indoption
            // Bit 0 indicates sort direction: 1 = DESC, 0 = ASC
            $columnName = $row['column_name'];
            $sortDirection = 'ASC'; // Default to ASC
            
            $optionValue = (int)($row['option_value'] ?? 0);
            if (($optionValue & 1) === 1) {
                $sortDirection = 'DESC';
            }
            
            $indexes[$indexName]['columns'][] = [
                'name' => $columnName,
                'direction' => $sortDirection,
            ];
        }

        return array_values($indexes);
    }

    private function getForeignKeys(PDO $pdo, string $table, string $schema = 'public'): array
    {
        $sql = "
            SELECT
                tc.constraint_name,
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name,
                ccu.table_schema AS foreign_table_schema,
                rc.update_rule,
                rc.delete_rule
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            JOIN information_schema.referential_constraints AS rc
                ON rc.constraint_name = tc.constraint_name
                AND rc.constraint_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema = :schema
            AND tc.table_name = :table
        ";

        $stmt = $pdo->prepare($sql);
        $stmt->execute(['schema' => $schema, 'table' => $table]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $foreignKeys = [];
        foreach ($rows as $row) {
            $constraintName = $row['constraint_name'];
            if (!isset($foreignKeys[$constraintName])) {
                $foreignKeys[$constraintName] = [
                    'name' => $constraintName,
                    'columns' => [],
                    'foreign_table' => $row['foreign_table_name'],
                    'foreign_table_schema' => $row['foreign_table_schema'] ?? $schema,
                    'foreign_columns' => [],
                    'on_update' => $this->convertReferentialAction($row['update_rule']),
                    'on_delete' => $this->convertReferentialAction($row['delete_rule']),
                ];
            }
            $foreignKeys[$constraintName]['columns'][] = $row['column_name'];
            $foreignKeys[$constraintName]['foreign_columns'][] = $row['foreign_column_name'];
        }

        return array_values($foreignKeys);
    }

    public function generateCreateTableStatement(string $table, array $schema): string
    {
        $columns = [];
        $primaryKeyColumns = $schema['primary_key']['columns'] ?? [];

        if (empty($schema['columns'])) {
            throw new \RuntimeException("Table {$table} has no columns - cannot create table without columns");
        }

        foreach ($schema['columns'] as $columnInfo) {
            $columnDef = $this->typeConverter->convertColumnDefinition($columnInfo);
            $columnName = $this->quoteIdentifier($columnInfo['column_name']);
            $type = $columnDef['type'];
            $nullable = $columnDef['nullable'] ? '' : ' NOT NULL';
            $default = $columnDef['default'] !== null ? ' DEFAULT ' . $columnDef['default'] : '';
            
            $columns[] = "  {$columnName} {$type}{$nullable}{$default}";
        }

        if (empty($columns)) {
            throw new \RuntimeException("Table {$table} has no valid columns after conversion");
        }

        $sql = "CREATE TABLE IF NOT EXISTS " . $this->quoteIdentifier($table) . " (\n";
        $sql .= implode(",\n", $columns);

        // Add primary key
        if (!empty($primaryKeyColumns)) {
            $pkColumns = array_map([$this, 'quoteIdentifier'], $primaryKeyColumns);
            $sql .= ",\n  PRIMARY KEY (" . implode(', ', $pkColumns) . ")";
        }

        $sql .= "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci";

        return $sql;
    }

    public function createIndexes(array $schemaMapping, bool $deferIndexes = true): void
    {
        if (!$deferIndexes) {
            return;
        }

        $this->logger->info('Creating indexes...');
        $targetPdo = $this->connectionManager->getTargetConnection();

        foreach ($schemaMapping as $table => $schema) {
            foreach ($schema['indexes'] as $index) {
                try {
                    $indexSql = $this->generateIndexStatement($table, $index);
                    
                    if (!$this->dryRun) {
                        $targetPdo->exec($indexSql);
                        $this->logger->info("Created index: {$index['name']} on {$table}");
                    } else {
                        $this->logger->info("DRY RUN - Would create index: {$index['name']}");
                    }
                } catch (\Exception $e) {
                    $this->logger->warning("Failed to create index {$index['name']} on {$table}: " . $e->getMessage());
                }
            }
        }

        $this->logger->success('Index creation completed');
    }

    public function createForeignKeys(array $schemaMapping): void
    {
        $this->logger->info('Creating foreign keys...');
        $targetPdo = $this->connectionManager->getTargetConnection();

        $successCount = 0;
        $failedCount = 0;

        foreach ($schemaMapping as $table => $schema) {
            foreach ($schema['foreign_keys'] as $fk) {
                try {
                    // Validate foreign key data before creating constraint
                    if (!$this->dryRun) {
                        $validationResult = $this->validateForeignKey($targetPdo, $table, $fk);
                        if (!$validationResult['valid']) {
                            $this->logger->warning("Skipping foreign key {$fk['name']} on {$table}: {$validationResult['message']}");
                            if (!empty($validationResult['orphaned_count'])) {
                                $this->logger->warning("  Found {$validationResult['orphaned_count']} orphaned row(s)");
                                if (!empty($validationResult['sample_values'])) {
                                    $samples = implode(', ', array_slice($validationResult['sample_values'], 0, 5));
                                    $this->logger->warning("  Sample orphaned values: {$samples}" . (count($validationResult['sample_values']) > 5 ? ' ...' : ''));
                                }
                            }
                            $failedCount++;
                            continue;
                        }
                    }

                    $fkSql = $this->generateForeignKeyStatement($table, $fk);
                    
                    if (!$this->dryRun) {
                        $targetPdo->exec($fkSql);
                        $this->logger->info("Created foreign key: {$fk['name']} on {$table}");
                        $successCount++;
                    } else {
                        $this->logger->info("DRY RUN - Would create foreign key: {$fk['name']}");
                    }
                } catch (\Exception $e) {
                    $this->logger->warning("Failed to create foreign key {$fk['name']} on {$table}: " . $e->getMessage());
                    $failedCount++;
                }
            }
        }

        if ($successCount > 0) {
            $this->logger->success("Foreign key creation completed: {$successCount} created");
        }
        if ($failedCount > 0) {
            $this->logger->warning("Foreign key creation: {$failedCount} skipped/failed (check logs for details)");
        }
    }

    private function validateForeignKey(PDO $pdo, string $table, array $fk): array
    {
        $tableName = $this->quoteIdentifier($table);
        $foreignTableName = $this->quoteIdentifier($fk['foreign_table']);
        
        // Build column lists
        $columns = array_map([$this, 'quoteIdentifier'], $fk['columns']);
        $foreignColumns = array_map([$this, 'quoteIdentifier'], $fk['foreign_columns']);
        
        $columnsList = implode(', ', $columns);
        $foreignColumnsList = implode(', ', $foreignColumns);
        
        // Check if foreign table exists
        $checkTableSql = "SELECT COUNT(*) FROM information_schema.tables 
                         WHERE table_schema = DATABASE() AND table_name = ?";
        $stmt = $pdo->prepare($checkTableSql);
        $stmt->execute([$fk['foreign_table']]);
        if ($stmt->fetchColumn() == 0) {
            return [
                'valid' => false,
                'message' => "Referenced table '{$fk['foreign_table']}' does not exist",
                'orphaned_count' => 0,
                'sample_values' => []
            ];
        }
        
        // Check for orphaned rows (non-NULL values that don't exist in referenced table)
        // Build WHERE clause to handle NULL values (NULLs are allowed in foreign keys)
        $whereConditions = [];
        foreach ($fk['columns'] as $col) {
            $quotedCol = $this->quoteIdentifier($col);
            $whereConditions[] = "{$quotedCol} IS NOT NULL";
        }
        $whereClause = implode(' AND ', $whereConditions);
        
        // Check for orphaned rows
        $checkSql = "
            SELECT COUNT(*) 
            FROM {$tableName} t
            WHERE {$whereClause}
            AND NOT EXISTS (
                SELECT 1 FROM {$foreignTableName} f
                WHERE " . $this->buildJoinCondition($fk['columns'], $fk['foreign_columns'], 't', 'f') . "
            )
        ";
        
        try {
            $stmt = $pdo->query($checkSql);
            $orphanedCount = (int) $stmt->fetchColumn();
            
            if ($orphanedCount > 0) {
                // Get sample orphaned values
                $sampleSql = "
                    SELECT " . $columnsList . "
                    FROM {$tableName} t
                    WHERE {$whereClause}
                    AND NOT EXISTS (
                        SELECT 1 FROM {$foreignTableName} f
                        WHERE " . $this->buildJoinCondition($fk['columns'], $fk['foreign_columns'], 't', 'f') . "
                    )
                    LIMIT 10
                ";
                $stmt = $pdo->query($sampleSql);
                $sampleRows = $stmt->fetchAll(PDO::FETCH_ASSOC);
                $sampleValues = [];
                foreach ($sampleRows as $row) {
                    $values = array_values($row);
                    $sampleValues[] = '(' . implode(', ', $values) . ')';
                }
                
                return [
                    'valid' => false,
                    'message' => "Found {$orphanedCount} orphaned row(s) with invalid foreign key values",
                    'orphaned_count' => $orphanedCount,
                    'sample_values' => $sampleValues
                ];
            }
        } catch (\Exception $e) {
            // If validation query fails, log warning but allow FK creation attempt
            $this->logger->warning("Could not validate foreign key {$fk['name']}: " . $e->getMessage());
            return [
                'valid' => true, // Allow attempt, will fail with better error if invalid
                'message' => '',
                'orphaned_count' => 0,
                'sample_values' => []
            ];
        }
        
        return [
            'valid' => true,
            'message' => '',
            'orphaned_count' => 0,
            'sample_values' => []
        ];
    }

    private function buildJoinCondition(array $columns, array $foreignColumns, string $tableAlias, string $foreignAlias): string
    {
        $conditions = [];
        for ($i = 0; $i < count($columns); $i++) {
            $col = $this->quoteIdentifier($columns[$i]);
            $foreignCol = $this->quoteIdentifier($foreignColumns[$i] ?? $foreignColumns[0]);
            $conditions[] = "{$tableAlias}.{$col} = {$foreignAlias}.{$foreignCol}";
        }
        return implode(' AND ', $conditions);
    }

    private function generateIndexStatement(string $table, array $index): string
    {
        $indexType = $this->typeConverter->convertIndexType($index['type']);
        $unique = $index['unique'] ? 'UNIQUE ' : '';
        $indexName = $this->quoteIdentifier($index['name']);
        $tableName = $this->quoteIdentifier($table);

        // Build column list with sort direction (ASC/DESC)
        $columnParts = [];
        foreach ($index['columns'] as $column) {
            if (is_array($column)) {
                // New format with direction
                $columnName = $this->quoteIdentifier($column['name']);
                $direction = $column['direction'] ?? 'ASC';
                $columnParts[] = "{$columnName} {$direction}";
            } else {
                // Legacy format (string column name) - default to ASC
                $columnName = $this->quoteIdentifier($column);
                $columnParts[] = "{$columnName} ASC";
            }
        }

        return "CREATE {$unique}INDEX {$indexName} ON {$tableName} (" . implode(', ', $columnParts) . ") USING {$indexType}";
    }

    private function generateForeignKeyStatement(string $table, array $fk): string
    {
        $columns = array_map([$this, 'quoteIdentifier'], $fk['columns']);
        $foreignColumns = array_map([$this, 'quoteIdentifier'], $fk['foreign_columns']);
        $tableName = $this->quoteIdentifier($table);
        $foreignTableName = $this->quoteIdentifier($fk['foreign_table']);
        $constraintName = $this->quoteIdentifier($fk['name']);

        return sprintf(
            "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s) ON UPDATE %s ON DELETE %s",
            $tableName,
            $constraintName,
            implode(', ', $columns),
            $foreignTableName,
            implode(', ', $foreignColumns),
            $fk['on_update'],
            $fk['on_delete']
        );
    }

    private function convertReferentialAction(string $action): string
    {
        return match (strtoupper($action)) {
            'CASCADE' => 'CASCADE',
            'SET NULL' => 'SET NULL',
            'SET DEFAULT' => 'SET DEFAULT',
            'RESTRICT' => 'RESTRICT',
            'NO ACTION' => 'NO ACTION',
            default => 'RESTRICT',
        };
    }

    private function quoteIdentifier(string $identifier): string
    {
        return '`' . str_replace('`', '``', $identifier) . '`';
    }

    private function quotePostgresIdentifier(string $identifier): string
    {
        return '"' . str_replace('"', '""', $identifier) . '"';
    }

    /**
     * Parse PostgreSQL array string into PHP array
     * Handles formats like: {1,2,3} or {1,NULL,3}
     */
    private function parsePostgresArray(?string $arrayString): array
    {
        if (empty($arrayString)) {
            return [];
        }

        // Remove curly braces
        $arrayString = trim($arrayString, '{}');
        
        if (empty($arrayString)) {
            return [];
        }

        // Split by comma, handling NULL values
        $elements = [];
        $parts = explode(',', $arrayString);
        
        foreach ($parts as $part) {
            $part = trim($part);
            if (strtoupper($part) === 'NULL') {
                $elements[] = null;
            } else {
                $elements[] = (int)$part;
            }
        }

        return $elements;
    }
}
