# PostgreSQL to MariaDB Migration Tool

A production-ready migration toolkit for migrating PostgreSQL databases to MariaDB, optimized for handling very large tables (20GB+) with minimal downtime during scheduled maintenance windows.

## Features

- **Handles Large Tables**: Chunked processing prevents memory issues with tables over 20GB
- **Memory-Aware Chunking**: Automatically adjusts chunk sizes based on PHP memory limit
- **Parallel Processing**: Multiple tables/chunks processed concurrently
- **Resumable**: Can resume from checkpoints if interrupted
- **Type Safety**: Comprehensive data type conversion from PostgreSQL to MariaDB
- **Date/Time Validation**: Automatically detects and fixes corrupted dates (defaults to Unix epoch)
- **Date Filtering**: Filter data migration by date ranges (after, before, or both)
- **SSH Tunnel Support**: Connect to databases through SSH tunnels for secure remote access
- **Table Filtering**: Selectively migrate specific tables or exclude tables via CLI or config
- **Foreign Key Validation**: Pre-validates foreign keys and reports orphaned rows before creation
- **Schema Support**: Migrate from specific PostgreSQL schemas (MySQL/MariaDB doesn't support schemas)
- **Skip Indexes Option**: Option to skip index creation for faster migration
- **Auto-Create Tables**: In data-only mode, automatically creates missing tables in target database
- **Missing Row Detection**: Find and report rows that failed to migrate
- **Validation**: Row count and sample data validation
- **Progress Tracking**: Real-time progress logging with single-line progress bars
- **Error Recovery**: Continues processing despite individual chunk failures
- **Dry Run Mode**: Test migration without making changes

## Requirements

- PHP 8.1 or higher
- PDO extensions:
  - `ext-pdo`
  - `ext-pdo_pgsql` (PostgreSQL)
  - `ext-pdo_mysql` (MySQL/MariaDB)
- SSH client (optional, for SSH tunnel support)
  - `ssh` command must be available in PATH
  - For password authentication: `sshpass` tool (optional, key-based auth recommended)

## Installation

1. Clone or download this repository
2. The project includes a custom autoloader, so no Composer installation is required
3. Configure the migration tool by creating a `.env` file (see Configuration section below)

## Configuration

### Environment Variables (.env file)

**Recommended:** Create a `.env` file in the project root with your database credentials. A `.env.example` file is provided as a template.

1. Copy the example file:
```bash
cp .env.example .env
```

2. Edit `.env` with your database credentials:
```env
# PostgreSQL Source Database Configuration
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=your_postgresql_database
PG_USERNAME=your_postgresql_username
PG_PASSWORD=your_postgresql_password
PG_SCHEMA=your_schema_name

# MySQL/MariaDB Target Database Configuration
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DATABASE=your_mysql_database
MYSQL_USERNAME=your_mysql_username
MYSQL_PASSWORD=your_mysql_password

# PostgreSQL SSH Tunnel Configuration (optional)
# Enable SSH tunneling if database is not directly accessible
PG_SSH_ENABLED=false
PG_SSH_HOST=ssh.example.com
PG_SSH_PORT=22
PG_SSH_USER=ssh_username
PG_SSH_KEY_PATH=/path/to/private/key
# OR use password authentication (requires sshpass)
# PG_SSH_PASSWORD=ssh_password
PG_SSH_REMOTE_HOST=localhost
PG_SSH_REMOTE_PORT=5432
PG_SSH_LOCAL_PORT=0

# MySQL/MariaDB SSH Tunnel Configuration (optional)
# Enable SSH tunneling if database is not directly accessible
MYSQL_SSH_ENABLED=false
MYSQL_SSH_HOST=ssh.example.com
MYSQL_SSH_PORT=22
MYSQL_SSH_USER=ssh_username
MYSQL_SSH_KEY_PATH=/path/to/private/key
# OR use password authentication (requires sshpass)
# MYSQL_SSH_PASSWORD=ssh_password
MYSQL_SSH_REMOTE_HOST=localhost
MYSQL_SSH_REMOTE_PORT=3306
MYSQL_SSH_LOCAL_PORT=0
```

**Important:** The `.env` file is excluded from Git (via `.gitignore`) to protect your credentials. Never commit your `.env` file to version control.

### Alternative: Environment Variables

You can also set the following environment variables directly (useful for CI/CD or Docker):

**Source Database (PostgreSQL):**
- `PG_HOST` - PostgreSQL host (default: localhost)
- `PG_PORT` - PostgreSQL port (default: 5432)
- `PG_DATABASE` - Database name
- `PG_USERNAME` - Username
- `PG_PASSWORD` - Password
- `PG_SCHEMA` - Specific schema to migrate from (default: null = all schemas). Since MySQL/MariaDB doesn't support schemas, you can specify which PostgreSQL schema to migrate.

**Target Database (MariaDB):**
- `MYSQL_HOST` - MariaDB host (default: localhost)
- `MYSQL_PORT` - MariaDB port (default: 3306)
- `MYSQL_DATABASE` - Database name
- `MYSQL_USERNAME` - Username
- `MYSQL_PASSWORD` - Password

**SSH Tunnel Configuration (Optional):**
- `PG_SSH_ENABLED` - Enable SSH tunnel for PostgreSQL (true/false, default: false)
- `PG_SSH_HOST` - SSH server hostname or IP
- `PG_SSH_PORT` - SSH server port (default: 22)
- `PG_SSH_USER` - SSH username
- `PG_SSH_KEY_PATH` - Path to SSH private key file (recommended)
- `PG_SSH_PASSWORD` - SSH password (alternative to key, requires `sshpass` tool)
- `PG_SSH_REMOTE_HOST` - Database host as seen from SSH server (default: localhost)
- `PG_SSH_REMOTE_PORT` - Database port as seen from SSH server (default: 5432)
- `PG_SSH_LOCAL_PORT` - Local port for tunnel (0 = auto-assign, default: 0)

- `MYSQL_SSH_ENABLED` - Enable SSH tunnel for MySQL/MariaDB (true/false, default: false)
- `MYSQL_SSH_HOST` - SSH server hostname or IP
- `MYSQL_SSH_PORT` - SSH server port (default: 22)
- `MYSQL_SSH_USER` - SSH username
- `MYSQL_SSH_KEY_PATH` - Path to SSH private key file (recommended)
- `MYSQL_SSH_PASSWORD` - SSH password (alternative to key, requires `sshpass` tool)
- `MYSQL_SSH_REMOTE_HOST` - Database host as seen from SSH server (default: localhost)
- `MYSQL_SSH_REMOTE_PORT` - Database port as seen from SSH server (default: 3306)
- `MYSQL_SSH_LOCAL_PORT` - Local port for tunnel (0 = auto-assign, default: 0)

### SSH Tunnel Configuration

The migration tool supports SSH tunneling for secure database connections when databases are not directly accessible. This is useful when:

- Databases are behind firewalls
- Databases only allow connections from specific hosts
- You need to connect through a bastion/jump server
- You want to encrypt database traffic over an insecure network

**How it works:**
1. The tool creates an SSH tunnel from your local machine to the SSH server
2. The SSH server forwards connections to the database server
3. The tool connects to the database through the local tunnel port
4. All database traffic is encrypted through the SSH tunnel

**Requirements:**
- SSH client must be installed (`ssh` command available)
- For password authentication: `sshpass` tool must be installed (optional, key-based auth recommended)
- SSH access to the server that can reach the database

**Example Configuration:**

```env
# Enable SSH tunnel for PostgreSQL
PG_SSH_ENABLED=true
PG_SSH_HOST=bastion.example.com
PG_SSH_PORT=22
PG_SSH_USER=deploy
PG_SSH_KEY_PATH=/home/user/.ssh/id_rsa
PG_SSH_REMOTE_HOST=localhost
PG_SSH_REMOTE_PORT=5432

# Enable SSH tunnel for MySQL/MariaDB
MYSQL_SSH_ENABLED=true
MYSQL_SSH_HOST=bastion.example.com
MYSQL_SSH_PORT=22
MYSQL_SSH_USER=deploy
MYSQL_SSH_KEY_PATH=/home/user/.ssh/id_rsa
MYSQL_SSH_REMOTE_HOST=localhost
MYSQL_SSH_REMOTE_PORT=3306
```

**Authentication Methods:**

1. **SSH Key (Recommended):**
   ```env
   PG_SSH_KEY_PATH=/path/to/private/key
   ```
   - More secure
   - No additional tools required
   - Supports passphrase-protected keys (via SSH agent)

2. **Password (Alternative):**
   ```env
   PG_SSH_PASSWORD=your_ssh_password
   ```
   - Requires `sshpass` tool to be installed
   - Less secure than key-based auth
   - Useful for quick testing or when keys aren't available

**Important Notes:**
- SSH tunnels are automatically created when connections are established
- Tunnels are automatically closed when the migration completes or is interrupted
- If a tunnel fails to establish, the migration will fail with a clear error message
- Multiple tunnels can be active simultaneously (one for source, one for target)
- Local ports are auto-assigned by default (starting from 33000) to avoid conflicts

**Migration Settings:**
- `MIGRATION_CHUNK_SIZE` - Default chunk size for data migration (default: 10000)
- `MIGRATION_LARGE_CHUNK_SIZE` - Chunk size for large tables (default: 50000)
- `MIGRATION_LARGE_TABLE_THRESHOLD_MB` - Threshold for large table detection (default: 1000)
- `MIGRATION_PARALLEL_WORKERS` - Number of parallel workers (default: 4)
- `MIGRATION_CHECKPOINT_INTERVAL` - Checkpoint frequency in chunks (default: 100)
- `MIGRATION_SKIP_INDEXES` - Skip index creation (true/false, default: false)

**Date Filtering (CLI-only options):**
Date filtering is currently only available via CLI options (`--after-date`, `--before-date`, `--date-column`). These cannot be set via environment variables in the current version.
- `MIGRATION_TABLES_INCLUDE` - Comma-separated list of tables to include (empty = all)
- `MIGRATION_TABLES_EXCLUDE` - Comma-separated list of tables to exclude

**Note:** You can also use CLI options to specify tables without modifying your `.env` file:
- `--tables TABLES` - Specify which tables to migrate (merged with `MIGRATION_TABLES_INCLUDE`). Works with `--full`, `--schema-only`, and `--data-only` modes.
- `--skip-tables TABLES` - Specify which tables to skip (merged with `MIGRATION_TABLES_EXCLUDE`). Works with `--full`, `--schema-only`, and `--data-only` modes.

**Behavior:**
- If both `--tables` and `MIGRATION_TABLES_INCLUDE` are specified, only tables that appear in both lists will be migrated (intersection).
- If both `--tables` and `--skip-tables` are specified, excluded tables take precedence (tables in `--skip-tables` will not be migrated even if they're in `--tables`).
- In `--data-only` mode, table filtering applies to both table discovery (for auto-creating missing tables) and data migration.

### Configuration File

Alternatively, edit `config/migration.php` directly:

```php
return [
    'source' => [
        'host' => 'localhost',
        'port' => 5432,
        'database' => 'your_pg_database',
        'username' => 'your_username',
        'password' => 'your_password',
        'schema' => 'ABEE', // Optional: specific schema to migrate (null = all schemas)
    ],
    'target' => [
        'host' => 'localhost',
        'port' => 3306,
        'database' => 'your_mysql_database',
        'username' => 'your_username',
        'password' => 'your_password',
    ],
    // ... migration settings
];
```

**Note on Schemas**: PostgreSQL supports multiple schemas within a database, but MySQL/MariaDB does not. If your PostgreSQL database has multiple schemas, you can:
- Set `schema` to a specific schema name  to migrate only that schema
- Set `schema` to `null` to migrate tables from all schemas (default)

## Usage

### Basic Usage

```bash
# Full migration (schema + data)
php migrate.php --full

# Schema only
php migrate.php --schema-only

# Data only (requires schema to exist, will auto-create missing tables)
php migrate.php --data-only

# Resume from checkpoint
php migrate.php --data-only --resume

# Dry run (test without making changes)
php migrate.php --dry-run --schema-only

# Skip index creation (faster migration)
php migrate.php --full --skip-indexes

# Migrate only rows after a specific date
php migrate.php --data-only --after-date '2024-01-01' --date-column 'created_at'

# Migrate only rows before a specific date
php migrate.php --data-only --before-date '2024-12-31' --date-column 'created_at'

# Migrate rows within a date range
php migrate.php --data-only --after-date '2024-01-01' --before-date '2024-12-31' --date-column 'created_at'

# Migrate only specific tables
php migrate.php --full --tables 'users,orders,products'

# Migrate only specific tables in data-only mode
php migrate.php --data-only --tables 'users,orders,products'

# Skip specific tables during migration
php migrate.php --full --skip-tables 'logs,audit_trail,temp_data'

# Skip specific tables in data-only mode
php migrate.php --data-only --skip-tables 'logs,audit_trail'

# Combine table filtering with date filtering
php migrate.php --data-only --tables 'transactions' --after-date '2024-01-01' --date-column 'created_at'

# Find missing rows (rows in source but not in target)
php migrate.php --find-missing
```

### Command Options

- `--full` - Perform full migration (schema + data) [default]
- `--schema-only` - Migrate schema only
- `--data-only` - Migrate data only (requires schema to exist, will auto-create missing tables)
- `--resume` - Resume from last checkpoint
- `--dry-run` - Test migration without making changes
- `--skip-indexes` - Skip creating indexes (faster migration, create manually later)
- `--tables TABLES` - Comma-separated list of tables to migrate (only these tables will be migrated, e.g., `'users,orders,products'`). Works with `--full`, `--schema-only`, and `--data-only` modes.
- `--skip-tables TABLES` - Comma-separated list of tables to skip during migration (e.g., `'logs,audit_trail'`). Works with `--full`, `--schema-only`, and `--data-only` modes.
- `--find-missing` - Find tables and rows that were not migrated
- `--after-date DATE` - Only migrate rows where date column is on or after DATE (includes DATE, requires `--date-column`)
- `--before-date DATE` - Only migrate rows where date column is before DATE (excludes DATE, requires `--date-column`)
- `--date-column COL` - Column name to use for date filtering (requires `--after-date` or `--before-date`)
- `--help` or `-h` - Show help message

### Date Filtering

The `--after-date`, `--before-date`, and `--date-column` options allow you to migrate only rows that meet date criteria. This is useful for:
- **Incremental migrations**: Migrate only new data since a specific date (inclusive)
- **Historical data**: Migrate only data before a specific date
- **Date range**: Migrate data within a specific date range
- **Selective migration**: Migrate only specific time periods

**Usage:**
```bash
# Migrate only rows where created_at is on or after 2024-01-01 (includes 2024-01-01)
php migrate.php --data-only --after-date '2024-01-01' --date-column 'created_at'

# Migrate only rows where created_at is before 2024-12-31 (excludes 2024-12-31)
php migrate.php --data-only --before-date '2024-12-31' --date-column 'created_at'

# Migrate rows within a date range (2024-01-01 to 2024-12-31, inclusive start, exclusive end)
php migrate.php --data-only --after-date '2024-01-01' --before-date '2024-12-31' --date-column 'created_at'

# With full timestamp
php migrate.php --full --after-date '2024-01-01 00:00:00' --date-column 'updated_at'
```

**Important Notes:**
- `--date-column` is required when using `--after-date` or `--before-date`
- You can use `--after-date` alone, `--before-date` alone, or both together for a date range
- `--after-date` uses `>=` (greater than or equal), so the specified date is **included**
- `--before-date` uses `<` (less than), so the specified date is **excluded**
- When using both filters together: `date_column >= after_date AND date_column < before_date`
- The date filter is applied at the SQL level for efficiency
- Row counts and progress bars reflect only filtered rows
- Works with both cursor-based and OFFSET pagination
- Date format: `YYYY-MM-DD` or `YYYY-MM-DD HH:MM:SS`

**Common Use Cases:**
- **Migrate recent data**: `--after-date '2024-01-01' --date-column 'created_at'`
- **Migrate historical data**: `--before-date '2024-01-01' --date-column 'created_at'`
- **Migrate specific year**: `--after-date '2024-01-01' --before-date '2025-01-01' --date-column 'created_at'`
- **Migrate specific month**: `--after-date '2024-01-01' --before-date '2024-02-01' --date-column 'created_at'`

### Date Filtering

The `--after-date`, `--before-date`, and `--date-column` options allow you to migrate only rows that meet date criteria. This is useful for:
- **Incremental migrations**: Migrate only new data since a specific date (inclusive)
- **Historical data**: Migrate only data before a specific date
- **Date range**: Migrate data within a specific date range
- **Selective migration**: Migrate only specific time periods

**Usage:**
```bash
# Migrate only rows where created_at is on or after 2024-01-01 (includes 2024-01-01)
php migrate.php --data-only --after-date '2024-01-01' --date-column 'created_at'

# Migrate only rows where created_at is before 2024-12-31 (excludes 2024-12-31)
php migrate.php --data-only --before-date '2024-12-31' --date-column 'created_at'

# Migrate rows within a date range (2024-01-01 to 2024-12-31, inclusive start, exclusive end)
php migrate.php --data-only --after-date '2024-01-01' --before-date '2024-12-31' --date-column 'created_at'

# With full timestamp
php migrate.php --full --after-date '2024-01-01 00:00:00' --date-column 'updated_at'
```

**Important Notes:**
- `--date-column` is required when using `--after-date` or `--before-date`
- You can use `--after-date` alone, `--before-date` alone, or both together for a date range
- `--after-date` uses `>=` (greater than or equal), so the specified date is **included**
- `--before-date` uses `<` (less than), so the specified date is **excluded**
- When using both filters together: `date_column >= after_date AND date_column < before_date`
- The date filter is applied at the SQL level for efficiency
- Row counts and progress bars reflect only filtered rows
- Works with both cursor-based and OFFSET pagination
- Date format: `YYYY-MM-DD` or `YYYY-MM-DD HH:MM:SS`

## Migration Process

The migration tool follows a multi-phase approach:

### Phase 1: Schema Migration
1. Connects to PostgreSQL and extracts schema (tables, columns, data types, constraints, indexes)
2. Converts PostgreSQL-specific data types to MariaDB equivalents
3. Generates MariaDB-compatible CREATE TABLE statements
4. Creates tables in MariaDB (without indexes initially for better performance)

### Phase 2: Data Migration (Chunked)
For each table:
1. Calculates total row count and table size
2. Determines optimal chunk size based on:
   - Table size (larger tables use larger chunks)
   - PHP memory limit (automatically adjusts to prevent memory exhaustion)
   - Conservative estimates ensure safe memory usage
3. Processes chunks:
   - Uses cursor-based pagination for large tables (more efficient than OFFSET)
   - Converts data types on-the-fly
   - Validates and fixes corrupted dates (invalid dates default to Unix epoch: 1970-01-01 00:00:00)
   - Inserts into MariaDB using batch inserts (split into smaller batches for memory safety)
   - Logs progress with single-line progress bars
4. Saves checkpoints periodically for resumability
5. Temporarily disables foreign key checks during data insertion for better performance

### Phase 3: Post-Migration
1. Creates indexes (deferred until after data load for better performance, optional with `--skip-indexes`)
2. Validates foreign key constraints before creation:
   - Checks for orphaned rows (child table values that don't exist in parent table)
   - Reports detailed information about problematic rows
   - Skips invalid foreign keys with warnings (migration continues)
3. Adds valid foreign key constraints
4. Updates AUTO_INCREMENT values
5. Validates data integrity (row counts and sample data)
6. Generates migration report

## Data Type Conversions

The tool automatically converts PostgreSQL data types to MariaDB equivalents:

| PostgreSQL | MariaDB |
|------------|---------|
| `SERIAL` | `INT AUTO_INCREMENT` |
| `BIGSERIAL` | `BIGINT AUTO_INCREMENT` |
| `TEXT` | `LONGTEXT` |
| `TIMESTAMP WITH TIME ZONE` | `DATETIME` |
| `TIMESTAMP WITHOUT TIME ZONE` | `DATETIME` |
| `DATE` | `DATE` |
| `TIME` | `TIME` |
| `JSONB` | `JSON` |
| `UUID` | `CHAR(36)` |
| `ARRAY[]` | `JSON` |
| `BYTEA` | `LONGBLOB` |
| `BOOLEAN` | `TINYINT(1)` |

### Date/Time Handling

The tool includes robust date/time validation:
- **Invalid Dates**: Corrupted dates (e.g., years with 5+ digits like "202511-11-13") are automatically detected
- **Default Value**: Invalid dates are converted to Unix epoch start: `1970-01-01 00:00:00`
- **Validation**: Dates are validated for reasonable year ranges (1900-2100)
- **Timezone Handling**: PostgreSQL timezone information is stripped (MariaDB DATETIME doesn't support timezones)

## Performance Considerations

### Memory Management

The tool automatically manages memory to prevent exhaustion:
- **Dynamic Chunk Sizing**: Chunk sizes are automatically calculated based on PHP's `memory_limit`
- **Conservative Estimates**: Uses only 20% of available memory for chunk data (leaves room for overhead)
- **Memory Per Row**: Estimates 4KB per row (conservative, accounts for all overhead)
- **Low Memory Systems**: For systems with 128MB or less, chunk size is capped at 2,000 rows
- **Batch Inserts**: Large chunks are split into smaller batches (default 1,000 rows) to reduce memory usage

### Large Tables (>20GB)

For very large tables, the tool automatically:
- Uses larger chunk sizes (up to configured `MIGRATION_LARGE_CHUNK_SIZE`, but respects memory limits)
- Uses cursor-based pagination instead of OFFSET (more efficient)
- Saves checkpoints more frequently
- Processes tables sequentially to avoid memory issues
- Splits large INSERT statements into smaller batches

### Optimization Tips

1. **Chunk Size**: The tool automatically adjusts, but you can configure:
   - `MIGRATION_CHUNK_SIZE`: Default for normal tables (default: 10,000)
   - `MIGRATION_LARGE_CHUNK_SIZE`: For large tables (default: 50,000)
   - Note: Actual chunk size will be limited by available memory

2. **Skip Indexes**: Use `--skip-indexes` flag for faster migration:
   ```bash
   php migrate.php --full --skip-indexes
   ```
   Create indexes manually after migration completes.

3. **Memory Limit**: Increase PHP memory if needed:
   ```bash
   php -d memory_limit=2G migrate.php --full
   ```

4. **Parallel Workers**: Adjust `MIGRATION_PARALLEL_WORKERS` based on CPU cores:
   - 4-8 workers for most scenarios
   - Reduce if database server is under heavy load

5. **Network**: Ensure good network connectivity between source and target databases

## Checkpoints and Resuming

The migration tool saves checkpoints periodically during data migration. If the migration is interrupted, you can resume from the last checkpoint:

```bash
php migrate.php --data-only --resume
```

Checkpoints are stored in the `migrations/` directory as JSON files. Each table has its own checkpoint file.

To clear all checkpoints and start fresh:
```bash
# Manually delete files in migrations/ directory
rm migrations/*_checkpoint.json
```

## Logging

Migration logs are stored in the `logs/` directory with timestamps. Each migration run creates a new log file:
- Format: `migration_YYYY-MM-DD_HHMMSS.log`
- Contains: Progress updates, errors, warnings, and validation results

## Troubleshooting

### Connection Issues

**Error: "Failed to connect to database"**
- Verify database credentials in configuration
- Check network connectivity
- Ensure PostgreSQL and MariaDB services are running
- Verify firewall rules allow connections

### Data Type Conversion Issues

**Error: "Invalid data type"**
- Check the log file for specific column/table details
- Some PostgreSQL-specific types may need manual conversion
- Review `src/Converters/TypeConverter.php` for conversion rules

### Memory Issues

**Error: "Allowed memory size exhausted"**
- The tool now automatically adjusts chunk sizes based on memory limit
- If you still encounter issues:
  - Increase PHP memory limit: `php -d memory_limit=2G migrate.php --full`
  - Reduce `MIGRATION_CHUNK_SIZE` in config (though auto-sizing should handle this)
  - Check log file for detected memory limit and calculated chunk size
  - Process tables one at a time if needed

### Large Table Performance

**Migration is very slow for large tables**
- Use `--skip-indexes` flag to skip index creation (create manually later)
- Increase `MIGRATION_LARGE_CHUNK_SIZE` (but note memory limits will still apply)
- Ensure indexes are not created during data load (they're deferred by default, or use `--skip-indexes`)
- Check network bandwidth between databases
- Consider running migration during off-peak hours
- Monitor log file for chunk size calculations and adjust PHP memory limit if needed

### Foreign Key Constraints

**Warning: "Skipping foreign key ... Found X orphaned row(s)"**
- The tool now validates foreign keys before creation
- Orphaned rows are detected and reported with sample values
- Foreign keys with orphaned data are skipped (migration continues)
- To fix:
  1. Check the log file for sample orphaned values
  2. Clean up orphaned data in source PostgreSQL database
  3. Re-run data migration for affected tables
  4. Or manually fix data in MariaDB after migration
  5. Create foreign keys manually after data is cleaned

**Error: "Cannot add foreign key constraint"**
- Ensure referenced tables exist in target database
- Check that referenced columns have matching data types
- Verify data integrity (no orphaned records)

## Validation

### Pre-Migration Validation

**Foreign Key Validation:**
- Before creating foreign key constraints, the tool validates data integrity
- Checks for orphaned rows (child table values that don't exist in parent table)
- Reports detailed information including:
  - Number of orphaned rows
  - Sample values of problematic rows
- Skips invalid foreign keys with warnings (migration continues)
- Allows you to fix data issues before constraint creation

**Date/Time Validation:**
- Automatically detects corrupted dates (e.g., invalid years, malformed formats)
- Invalid dates are converted to Unix epoch (`1970-01-01 00:00:00`)
- Validates year ranges (1900-2100)
- Handles timezone conversion from PostgreSQL to MariaDB

### Post-Migration Validation

After migration, the tool automatically validates:
- Row counts match between source and target
- Sample data matches (first 100 rows)

Validation results are logged and displayed. If validation fails:
1. Check the log file for details
2. Review specific tables that failed
3. Consider re-running data migration for failed tables

## Limitations

- **Stored Procedures/Functions**: Not automatically migrated (manual conversion required)
- **Triggers**: Not automatically migrated (manual conversion required)
- **Views**: Not automatically migrated (manual conversion required)
- **PostgreSQL-specific features**: Some features may need manual conversion:
  - Full-text search indexes
  - Custom data types
  - Advanced PostgreSQL functions

## Best Practices

1. **Backup First**: Always backup both source and target databases before migration
2. **Test First**: Run with `--dry-run` to test the migration process
3. **Schema First**: Migrate schema separately first, review, then migrate data
4. **Monitor Progress**: Watch log files during migration
5. **Validate**: Always run validation after migration
6. **Maintenance Window**: Schedule migration during maintenance windows
7. **Resource Planning**: Ensure adequate resources (CPU, memory, disk, network)

## Example Workflow

```bash
# 1. Test connection and configuration
php migrate.php --dry-run --schema-only

# 2. Migrate schema
php migrate.php --schema-only

# 3. Review schema in target database

# 4. Migrate data (with skip-indexes for faster migration)
php migrate.php --data-only --skip-indexes

# 5. If interrupted, resume
php migrate.php --data-only --resume

# 6. Create indexes manually (if skipped)
# (Connect to MariaDB and create indexes as needed)

# 7. Validate migration
# (Validation runs automatically after full migration)
```

### Alternative: Full Migration with Indexes

```bash
# Full migration including indexes
php migrate.php --full

# Or skip indexes for faster migration
php migrate.php --full --skip-indexes
```

### Selective Table Migration

```bash
# Migrate only specific tables (full migration)
php migrate.php --full --tables 'users,orders,products'

# Migrate only specific tables (data-only mode)
php migrate.php --data-only --tables 'users,orders,products'

# Skip specific tables during migration
php migrate.php --full --skip-tables 'logs,audit_trail,temp_data'

# Combine table filtering with date filtering
php migrate.php --data-only --tables 'transactions' --after-date '2024-01-01' --date-column 'created_at'

# Use both include and exclude (exclude takes precedence)
php migrate.php --full --tables 'users,orders,products,logs' --skip-tables 'logs'
# Result: Only 'users', 'orders', and 'products' will be migrated (logs is excluded)
```

### Incremental Migration (Date Filtering)

```bash
# 1. Initial full migration
php migrate.php --full

# 2. Later, migrate only new data since a specific date
php migrate.php --data-only --after-date '2024-01-01' --date-column 'created_at'

# 3. Or migrate only historical data before a specific date
php migrate.php --data-only --before-date '2024-01-01' --date-column 'created_at'

# 4. Or migrate data within a specific date range
php migrate.php --data-only --after-date '2024-01-01' --before-date '2024-12-31' --date-column 'created_at'

# 5. Combine with table filtering for specific tables
php migrate.php --data-only --tables 'transactions,orders' --after-date '2024-01-01' --date-column 'created_at'

# 6. Check for any missing rows
php migrate.php --find-missing
```

### Finding Missing Rows

After migration, you can identify any rows that didn't migrate successfully:

```bash
# Find all missing rows
php migrate.php --find-missing

# This will show:
# - Tables with row count mismatches
# - Sample missing rows (up to 100 per table)
# - Actual row data that's missing
```

## Support

For issues, questions, or contributions, please refer to the project repository.

## License

This tool is provided as-is for database migration purposes.
