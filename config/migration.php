<?php

declare(strict_types=1);

return [
    'source' => [
        'driver' => 'pgsql',
        'host' => env('PG_HOST', 'localhost'),
        'port' => env('PG_PORT', 5432),
        'database' => env('PG_DATABASE', ''),
        'username' => env('PG_USERNAME', ''),
        'password' => env('PG_PASSWORD', ''),
        'charset' => 'utf8',
        'schema' => env('PG_SCHEMA', null), // Specific schema to migrate from (null = all schemas)
        'ssh' => [
            'enabled' => (bool) env('PG_SSH_ENABLED', false),
            'host' => env('PG_SSH_HOST', ''),
            'port' => (int) env('PG_SSH_PORT', 22),
            'user' => env('PG_SSH_USER', ''),
            'key_path' => env('PG_SSH_KEY_PATH', null),
            'password' => env('PG_SSH_PASSWORD', null),
            'remote_host' => env('PG_SSH_REMOTE_HOST', 'localhost'), // Database host as seen from SSH server
            'remote_port' => (int) env('PG_SSH_REMOTE_PORT', 5432), // Database port as seen from SSH server
            'local_port' => (int) env('PG_SSH_LOCAL_PORT', 0), // Local port for tunnel (0 = auto-assign)
        ],
    ],

    'target' => [
        'driver' => 'mysql',
        'host' => env('MYSQL_HOST', 'localhost'),
        'port' => env('MYSQL_PORT', 3306),
        'database' => env('MYSQL_DATABASE', ''),
        'username' => env('MYSQL_USERNAME', ''),
        'password' => env('MYSQL_PASSWORD', ''),
        'charset' => 'utf8mb4',
        'collation' => 'utf8mb4_unicode_ci',
        'ssh' => [
            'enabled' => (bool) env('MYSQL_SSH_ENABLED', false),
            'host' => env('MYSQL_SSH_HOST', ''),
            'port' => (int) env('MYSQL_SSH_PORT', 22),
            'user' => env('MYSQL_SSH_USER', ''),
            'key_path' => env('MYSQL_SSH_KEY_PATH', null),
            'password' => env('MYSQL_SSH_PASSWORD', null),
            'remote_host' => env('MYSQL_SSH_REMOTE_HOST', 'localhost'), // Database host as seen from SSH server
            'remote_port' => (int) env('MYSQL_SSH_REMOTE_PORT', 3306), // Database port as seen from SSH server
            'local_port' => (int) env('MYSQL_SSH_LOCAL_PORT', 0), // Local port for tunnel (0 = auto-assign)
        ],
    ],

    'migration' => [
        'chunk_size' => (int) env('MIGRATION_CHUNK_SIZE', 10000),
        'large_table_chunk_size' => (int) env('MIGRATION_LARGE_CHUNK_SIZE', 50000),
        'large_table_threshold_mb' => (int) env('MIGRATION_LARGE_TABLE_THRESHOLD_MB', 1000),
        'parallel_workers' => (int) env('MIGRATION_PARALLEL_WORKERS', 4),
        'checkpoint_interval' => (int) env('MIGRATION_CHECKPOINT_INTERVAL', 100),
        'skip_indexes' => (bool) env('MIGRATION_SKIP_INDEXES', false),
        'tables_include' => env('MIGRATION_TABLES_INCLUDE', '') ? explode(',', env('MIGRATION_TABLES_INCLUDE')) : [],
        'tables_exclude' => env('MIGRATION_TABLES_EXCLUDE', '') ? explode(',', env('MIGRATION_TABLES_EXCLUDE')) : [],
    ],

    'paths' => [
        'migrations' => __DIR__ . '/../migrations',
        'logs' => __DIR__ . '/../logs',
    ],
];
