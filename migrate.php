#!/usr/bin/env php
<?php

declare(strict_types=1);

/**
 * PostgreSQL to MariaDB Migration Tool
 * 
 * Entry point for the migration process
 */

require_once __DIR__ . '/vendor/autoload.php';

use Migration\Commands\MigrateCommand;

// Load .env file if it exists
$envFile = __DIR__ . '/.env';
if (file_exists($envFile)) {
    $lines = file($envFile, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
    foreach ($lines as $line) {
        // Skip comments
        if (strpos(trim($line), '#') === 0) {
            continue;
        }
        // Parse KEY=VALUE format
        if (strpos($line, '=') !== false) {
            [$key, $value] = explode('=', $line, 2);
            $key = trim($key);
            $value = trim($value);
            // Remove quotes if present
            if ((substr($value, 0, 1) === '"' && substr($value, -1) === '"') ||
                (substr($value, 0, 1) === "'" && substr($value, -1) === "'")) {
                $value = substr($value, 1, -1);
            }
            if (!empty($key)) {
                $_ENV[$key] = $value;
                $_SERVER[$key] = $value;
                putenv("{$key}={$value}");
            }
        }
    }
}

// Simple environment variable loader
function env(string $key, mixed $default = null): mixed
{
    $value = $_ENV[$key] ?? $_SERVER[$key] ?? getenv($key);
    
    if ($value === false) {
        return $default;
    }
    
    // Convert string booleans
    if (strtolower($value) === 'true') {
        return true;
    }
    if (strtolower($value) === 'false') {
        return false;
    }
    
    return $value;
}

// Load configuration
$configFile = __DIR__ . '/config/migration.php';
if (!file_exists($configFile)) {
    echo "Error: Configuration file not found at {$configFile}\n";
    echo "Please create the configuration file or set environment variables.\n";
    exit(1);
}

$config = require $configFile;

// Parse command line arguments
$options = MigrateCommand::parseArguments($argv);

// Display usage if help requested
if (in_array('--help', $argv) || in_array('-h', $argv)) {
    echo "PostgreSQL to MariaDB Migration Tool\n\n";
    echo "Usage: php migrate.php [OPTIONS]\n\n";
    echo "Options:\n";
    echo "  --full          Perform full migration (schema + data) [default]\n";
    echo "  --schema-only   Migrate schema only\n";
    echo "  --data-only     Migrate data only (requires schema to exist)\n";
    echo "  --resume        Resume from last checkpoint\n";
    echo "  --dry-run       Test migration without making changes\n";
    echo "  --skip-indexes  Skip creating indexes (faster migration, create manually later)\n";
    echo "  --help, -h      Show this help message\n\n";
    echo "Environment Variables:\n";
    echo "  PG_HOST, PG_PORT, PG_DATABASE, PG_USERNAME, PG_PASSWORD\n";
    echo "  MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE, MYSQL_USERNAME, MYSQL_PASSWORD\n";
    echo "  MIGRATION_CHUNK_SIZE, MIGRATION_PARALLEL_WORKERS, etc.\n\n";
    exit(0);
}

// Validate required configuration
$requiredSource = ['host', 'database', 'username', 'password'];
$requiredTarget = ['host', 'database', 'username', 'password'];

foreach ($requiredSource as $key) {
    if (empty($config['source'][$key])) {
        echo "Error: Missing required source database configuration: {$key}\n";
        echo "Set PG_" . strtoupper($key) . " environment variable or update config/migration.php\n";
        exit(1);
    }
}

foreach ($requiredTarget as $key) {
    if (empty($config['target'][$key])) {
        echo "Error: Missing required target database configuration: {$key}\n";
        echo "Set MYSQL_" . strtoupper($key) . " environment variable or update config/migration.php\n";
        exit(1);
    }
}

try {
    $command = new MigrateCommand($config, $options['dry-run'], $options['skip-indexes']);
    $command->execute($options['mode'], $options['resume']);
    
    echo "\nMigration completed successfully!\n";
    exit(0);
} catch (\Exception $e) {
    echo "\nError: " . $e->getMessage() . "\n";
    if (isset($options['verbose']) || in_array('--verbose', $argv)) {
        echo "\nStack trace:\n" . $e->getTraceAsString() . "\n";
    }
    exit(1);
}
