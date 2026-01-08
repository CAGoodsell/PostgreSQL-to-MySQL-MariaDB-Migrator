<?php

declare(strict_types=1);

namespace Migration\Commands;

use Migration\Converters\TypeConverter;
use Migration\Database\ConnectionManager;
use Migration\Logger\ProgressLogger;
use Migration\Migrators\DataMigrator;
use Migration\Migrators\SchemaMigrator;
use Migration\Validators\DataValidator;
use PDO;

class MigrateCommand
{
    private array $config;
    private ConnectionManager $connectionManager;
    private ProgressLogger $logger;
    private TypeConverter $typeConverter;
    private bool $dryRun;
    private bool $skipIndexes;
    private ?string $afterDate;
    private ?string $beforeDate;
    private ?string $dateColumn;
    private ?string $skipTables;
    private ?string $includeTables;

    public function __construct(array $config, bool $dryRun = false, bool $skipIndexes = false, ?string $afterDate = null, ?string $beforeDate = null, ?string $dateColumn = null, ?string $skipTables = null, ?string $includeTables = null)
    {
        $this->config = $config;
        $this->dryRun = $dryRun;
        $this->skipIndexes = $skipIndexes;
        $this->afterDate = $afterDate;
        $this->beforeDate = $beforeDate;
        $this->dateColumn = $dateColumn;
        $this->skipTables = $skipTables;
        $this->includeTables = $includeTables;

        $this->connectionManager = new ConnectionManager([
            'source' => $config['source'],
            'target' => $config['target'],
        ]);

        $this->logger = new ProgressLogger(
            $config['paths']['logs'],
            $config['paths']['migrations']
        );

        $this->typeConverter = new TypeConverter();
    }

    public function execute(string $mode = 'full', bool $resume = false): void
    {
        try {
            $this->logger->info('=== PostgreSQL to MariaDB Migration Tool ===');
            $this->logger->info('Mode: ' . strtoupper($mode));

            if ($this->dryRun) {
                $this->logger->warning('DRY RUN MODE - No changes will be made');
            }

            // Test connections
            $this->logger->info('Testing database connections...');
            $this->connectionManager->testConnections();
            $this->logger->success('Database connections successful');

            $schemaMapping = [];

            // Schema migration
            if ($mode === 'full' || $mode === 'schema-only') {
                $schemaMigrator = new SchemaMigrator(
                    $this->connectionManager,
                    $this->typeConverter,
                    $this->logger,
                    $this->dryRun
                );

                // Merge CLI included/excluded tables with config
                $tablesInclude = $this->getIncludedTables();
                $tablesExclude = $this->getExcludedTables();

                $schemaMapping = $schemaMigrator->migrateSchema(
                    $tablesInclude,
                    $tablesExclude,
                    $this->config['source']['schema'] ?? null
                );
            }

            // Data migration
            if ($mode === 'full' || $mode === 'data-only') {
                // In data-only mode, check if tables exist and create them if missing
                if ($mode === 'data-only' && empty($schemaMapping)) {
                    $this->logger->info('Data-only mode: Checking if tables exist in target database...');

                    // Get list of tables that need to be migrated
                    $sourcePdo = $this->connectionManager->getSourceConnection();
                    $targetPdo = $this->connectionManager->getTargetConnection();

                    $schemaMigrator = new SchemaMigrator(
                        $this->connectionManager,
                        $this->typeConverter,
                        $this->logger,
                        $this->dryRun
                    );

                    // Get tables from source
                    $tablesInclude = $this->getIncludedTables();
                    $tablesExclude = $this->getExcludedTables();

                    $tablesWithSchemas = $schemaMigrator->getTables(
                        $sourcePdo,
                        $tablesInclude,
                        $tablesExclude,
                        $this->config['source']['schema'] ?? null
                    );

                    // Check which tables are missing in target
                    $missingTables = [];
                    foreach ($tablesWithSchemas as $tableInfo) {
                        $table = $tableInfo['table'];
                        if (!$this->tableExists($targetPdo, $table)) {
                            $missingTables[] = $tableInfo;
                        }
                    }

                    // Create missing tables
                    if (!empty($missingTables)) {
                        $this->logger->info('Found ' . count($missingTables) . ' table(s) that do not exist in target database. Creating them...');

                        // Create schema mapping for missing tables only
                        $schemaMapping = [];
                        foreach ($missingTables as $tableInfo) {
                            $table = $tableInfo['table'];
                            $schema = $tableInfo['schema'];

                            try {
                                $tableSchema = $schemaMigrator->extractTableSchema($sourcePdo, $table, $schema);

                                if (empty($tableSchema['columns'])) {
                                    $this->logger->warning("Table {$schema}.{$table} has no columns - skipping");
                                    continue;
                                }

                                $createStatement = $schemaMigrator->generateCreateTableStatement($table, $tableSchema);

                                $schemaMapping[$table] = [
                                    'schema' => $schema,
                                    'columns' => $tableSchema['columns'],
                                    'indexes' => $tableSchema['indexes'],
                                    'foreign_keys' => $tableSchema['foreign_keys'],
                                ];

                                if (!$this->dryRun) {
                                    $targetPdo->exec($createStatement);
                                    $this->logger->success("Created missing table: {$table}");
                                } else {
                                    $this->logger->info("DRY RUN - Would create missing table: {$table}");
                                }
                            } catch (\Exception $e) {
                                $this->logger->error("Failed to create missing table {$schema}.{$table}: " . $e->getMessage());
                                throw $e;
                            }
                        }

                        if (!empty($schemaMapping)) {
                            $this->logger->success('Created ' . count($schemaMapping) . ' missing table(s)');
                        }
                    } else {
                        $this->logger->info('All tables already exist in target database');
                    }
                }

                $dataMigrator = new DataMigrator(
                    $this->connectionManager,
                    $this->typeConverter,
                    $this->logger,
                    $this->config['migration'],
                    $schemaMapping,
                    $this->dryRun
                );

                // Use command-line arguments if provided, otherwise use config
                $afterDate = $this->afterDate ?? $this->config['migration']['after_date'] ?? null;
                $beforeDate = $this->beforeDate ?? $this->config['migration']['before_date'] ?? null;
                $dateColumn = $this->dateColumn ?? $this->config['migration']['date_column'] ?? null;

                // Validate date filter arguments
                if (($afterDate !== null || $beforeDate !== null) && $dateColumn === null) {
                    $this->logger->error('--after-date or --before-date requires --date-column to be specified');
                    throw new \RuntimeException('Date filters require --date-column');
                }
                if ($dateColumn !== null && $afterDate === null && $beforeDate === null) {
                    $this->logger->error('--date-column requires --after-date or --before-date to be specified');
                    throw new \RuntimeException('--date-column requires a date filter');
                }

                $tablesInclude = $this->getIncludedTables();
                $tablesExclude = $this->getExcludedTables();

                $dataMigrator->migrateData(
                    $tablesInclude,
                    $tablesExclude,
                    $resume,
                    $this->config['source']['schema'] ?? null,
                    $afterDate,
                    $beforeDate,
                    $dateColumn
                );
            }

            // Create indexes and foreign keys (deferred)
            if ($mode === 'full' || $mode === 'schema-only') {
                if (!empty($schemaMapping)) {
                    $schemaMigrator = new SchemaMigrator(
                        $this->connectionManager,
                        $this->typeConverter,
                        $this->logger,
                        $this->dryRun
                    );

                    // Skip indexes if requested (via CLI flag or config)
                    $shouldSkipIndexes = $this->skipIndexes || ($this->config['migration']['skip_indexes'] ?? false);

                    if ($shouldSkipIndexes) {
                        $this->logger->info('Skipping index creation (--skip-indexes flag or config option set)');
                    } else {
                        $this->logger->info('Creating indexes (deferred)...');
                        $schemaMigrator->createIndexes($schemaMapping, true);
                    }

                    $this->logger->info('Creating foreign keys...');
                    $schemaMigrator->createForeignKeys($schemaMapping);
                }
            }

            // Validation
            if ($mode === 'full' && !$this->dryRun) {
                $tablesInclude = $this->getIncludedTables();
                $tablesExclude = $this->getExcludedTables();

                $validator = new DataValidator($this->connectionManager, $this->logger);
                $results = $validator->validateMigration(
                    $tablesInclude,
                    $tablesExclude
                );

                $failed = array_filter($results, fn($r) => !($r['valid'] ?? false));
                if (!empty($failed)) {
                    $this->logger->error('Validation failed for ' . count($failed) . ' table(s)');
                    foreach ($failed as $table => $result) {
                        $this->logger->error("Table {$table}: " . ($result['error'] ?? 'Validation failed'));
                    }
                } else {
                    $this->logger->success('All tables validated successfully');
                }
            }

            $this->logger->success('=== Migration completed ===');
            $this->logger->info("Log file: {$this->logger->getLogFile()}");
        } catch (\Exception $e) {
            $this->logger->error('Migration failed: ' . $e->getMessage());
            $this->logger->error('Stack trace: ' . $e->getTraceAsString());
            throw $e;
        } finally {
            $this->connectionManager->closeConnections();
        }
    }

    public static function parseArguments(array $argv): array
    {
        $options = [
            'mode' => 'full',
            'resume' => false,
            'dry-run' => false,
            'skip-indexes' => false,
            'find-missing' => false,
            'after-date' => null,
            'before-date' => null,
            'date-column' => null,
            'skip-tables' => null,
            'tables' => null,
        ];

        foreach ($argv as $index => $arg) {
            if ($arg === '--schema-only') {
                $options['mode'] = 'schema-only';
            } elseif ($arg === '--data-only') {
                $options['mode'] = 'data-only';
            } elseif ($arg === '--full') {
                $options['mode'] = 'full';
            } elseif ($arg === '--resume') {
                $options['resume'] = true;
            } elseif ($arg === '--dry-run') {
                $options['dry-run'] = true;
            } elseif ($arg === '--skip-indexes') {
                $options['skip-indexes'] = true;
            } elseif ($arg === '--find-missing') {
                $options['find-missing'] = true;
            } elseif ($arg === '--after-date' && isset($argv[$index + 1])) {
                $options['after-date'] = $argv[$index + 1];
            } elseif ($arg === '--before-date' && isset($argv[$index + 1])) {
                $options['before-date'] = $argv[$index + 1];
            } elseif ($arg === '--date-column' && isset($argv[$index + 1])) {
                $options['date-column'] = $argv[$index + 1];
            } elseif ($arg === '--skip-tables' && isset($argv[$index + 1])) {
                $options['skip-tables'] = $argv[$index + 1];
            } elseif ($arg === '--tables' && isset($argv[$index + 1])) {
                $options['tables'] = $argv[$index + 1];
            }
        }

        return $options;
    }

    public function findMissingRows(array $tablesInclude = [], array $tablesExclude = [], ?string $sourceSchema = null): void
    {
        try {
            $this->logger->info('=== Finding Missing Rows ===');

            // Test connections
            $this->logger->info('Testing database connections...');
            $this->connectionManager->testConnections();
            $this->logger->success('Database connections successful');

            // Merge CLI included/excluded tables with provided tables
            $mergedInclude = $this->getIncludedTables();
            if (!empty($tablesInclude)) {
                // If both CLI and provided include lists exist, use intersection
                if (!empty($mergedInclude)) {
                    $mergedInclude = array_intersect($mergedInclude, $tablesInclude);
                } else {
                    $mergedInclude = $tablesInclude;
                }
            }

            $mergedExclude = array_merge($tablesExclude, $this->getExcludedTables());
            $mergedExclude = array_values(array_unique(array_filter($mergedExclude)));

            $validator = new DataValidator($this->connectionManager, $this->logger);
            $results = $validator->findMissingRows(
                $mergedInclude,
                $mergedExclude,
                $sourceSchema,
                100 // Limit per table
            );

            // Display results
            $this->logger->info("\n=== Missing Rows Report ===");
            foreach ($results as $table => $result) {
                if (isset($result['error'])) {
                    $this->logger->error("Table {$table}: {$result['error']}");
                    continue;
                }

                if (empty($result['missing_rows'])) {
                    continue;
                }

                $this->logger->error("\nTable: {$table}");
                $this->logger->error("  Source rows: {$result['source_rows']}");
                $this->logger->error("  Target rows: {$result['target_rows']}");
                $this->logger->error("  Missing rows: {$result['missing_count']}");

                // Display sample missing rows
                $sampleSize = min(5, count($result['missing_rows']));
                $this->logger->error("  Sample missing rows (showing {$sampleSize} of {$result['missing_count']}):");
                for ($i = 0; $i < $sampleSize; $i++) {
                    $row = $result['missing_rows'][$i];
                    $rowStr = json_encode($row, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE);
                    $this->logger->error("    Row " . ($i + 1) . ": " . $rowStr);
                }
            }

            $tablesWithMissing = count(array_filter($results, fn($r) => !empty($r['missing_rows'] ?? [])));
            if ($tablesWithMissing === 0) {
                $this->logger->success("\nNo missing rows found! All data appears to be migrated.");
            } else {
                $this->logger->error("\nFound missing rows in {$tablesWithMissing} table(s)");
            }
        } catch (\Exception $e) {
            $this->logger->error('Failed to find missing rows: ' . $e->getMessage());
            $this->logger->error('Stack trace: ' . $e->getTraceAsString());
            throw $e;
        } finally {
            $this->connectionManager->closeConnections();
        }
    }

    /**
     * Get included tables (merge CLI and config)
     */
    private function getIncludedTables(): array
    {
        $included = $this->config['migration']['tables_include'] ?? [];

        // If CLI specifies tables, use those (overrides config)
        if ($this->includeTables !== null && $this->includeTables !== '') {
            $cliIncluded = array_map('trim', explode(',', $this->includeTables));
            // If CLI provides tables, use only those (merge with config if both exist)
            if (!empty($included)) {
                // If both CLI and config have tables, use intersection (tables in both)
                $included = array_intersect($included, $cliIncluded);
            } else {
                // If only CLI has tables, use CLI tables
                $included = $cliIncluded;
            }
        }

        // Remove duplicates and empty values
        $included = array_filter(array_unique($included));

        return array_values($included);
    }

    /**
     * Get excluded tables (merge CLI and config)
     */
    private function getExcludedTables(): array
    {
        $excluded = $this->config['migration']['tables_exclude'] ?? [];

        // Add CLI excluded tables
        if ($this->skipTables !== null && $this->skipTables !== '') {
            $cliExcluded = array_map('trim', explode(',', $this->skipTables));
            $excluded = array_merge($excluded, $cliExcluded);
        }

        // Remove duplicates and empty values
        $excluded = array_filter(array_unique($excluded));

        return array_values($excluded);
    }

    /**
     * Check if a table exists in the target database
     */
    private function tableExists(PDO $pdo, string $table): bool
    {
        try {
            $stmt = $pdo->prepare("
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = DATABASE() 
                AND table_name = ?
            ");
            $stmt->execute([$table]);
            return (int) $stmt->fetchColumn() > 0;
        } catch (\Exception $e) {
            // If query fails, assume table doesn't exist
            return false;
        }
    }
}
