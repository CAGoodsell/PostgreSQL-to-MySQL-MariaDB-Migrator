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

    public function __construct(array $config, bool $dryRun = false, bool $skipIndexes = false)
    {
        $this->config = $config;
        $this->dryRun = $dryRun;
        $this->skipIndexes = $skipIndexes;
        
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

                $schemaMapping = $schemaMigrator->migrateSchema(
                    $this->config['migration']['tables_include'],
                    $this->config['migration']['tables_exclude'],
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
                    $tablesWithSchemas = $schemaMigrator->getTables(
                        $sourcePdo,
                        $this->config['migration']['tables_include'],
                        $this->config['migration']['tables_exclude'],
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

                $dataMigrator->migrateData(
                    $this->config['migration']['tables_include'],
                    $this->config['migration']['tables_exclude'],
                    $resume,
                    $this->config['source']['schema'] ?? null
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
                $validator = new DataValidator($this->connectionManager, $this->logger);
                $results = $validator->validateMigration(
                    $this->config['migration']['tables_include'],
                    $this->config['migration']['tables_exclude']
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
        ];

        foreach ($argv as $arg) {
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
            }
        }

        return $options;
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
