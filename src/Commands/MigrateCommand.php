<?php

declare(strict_types=1);

namespace Migration\Commands;

use Migration\Converters\TypeConverter;
use Migration\Database\ConnectionManager;
use Migration\Logger\ProgressLogger;
use Migration\Migrators\DataMigrator;
use Migration\Migrators\SchemaMigrator;
use Migration\Validators\DataValidator;

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
                if (empty($schemaMapping) && $mode === 'data-only') {
                    // Load schema mapping from checkpoint or require schema migration first
                    $this->logger->warning('Schema mapping not available. Attempting to load from checkpoints...');
                    // For data-only mode, we'd need to load schema from target database
                    // This is a simplified implementation
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
}
