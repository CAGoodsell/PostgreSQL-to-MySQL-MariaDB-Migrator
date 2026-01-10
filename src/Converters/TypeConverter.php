<?php

declare(strict_types=1);

namespace Migration\Converters;

class TypeConverter
{
    /**
     * Convert PostgreSQL data type to MariaDB equivalent
     */
    public function convertDataType(string $pgType, ?int $length = null, ?int $precision = null, ?int $scale = null): string
    {
        // Normalize PostgreSQL type (remove length/precision info)
        $baseType = strtolower(preg_replace('/\([^)]*\)/', '', $pgType));
        $baseType = trim($baseType);

        return match ($baseType) {
            // Integer types
            'smallint' => 'SMALLINT',
            'integer', 'int' => 'INT',
            'bigint' => 'BIGINT',
            'serial' => 'INT AUTO_INCREMENT',
            'bigserial' => 'BIGINT AUTO_INCREMENT',
            'smallserial' => 'SMALLINT AUTO_INCREMENT',

            // Decimal types
            'decimal', 'numeric' => $this->formatDecimal($precision, $scale),
            'real' => 'FLOAT',
            'double precision' => 'DOUBLE',

            // Character types
            'character varying', 'varchar' => $this->formatVarchar($length),
            'character', 'char' => $this->formatChar($length),
            'text' => 'LONGTEXT',

            // Binary types
            'bytea' => 'LONGBLOB',

            // Date/time types
            'date' => 'DATE',
            'time without time zone', 'time' => 'TIME',
            'time with time zone' => 'TIME',
            'timestamp without time zone', 'timestamp' => 'DATETIME',
            'timestamp with time zone', 'timestamptz' => 'DATETIME',
            'interval' => 'TIME',

            // Boolean
            'boolean', 'bool' => 'BOOLEAN',

            // JSON types
            'json' => 'JSON',
            'jsonb' => 'JSON',

            // UUID
            'uuid' => 'CHAR(36)',

            // Array types (convert to JSON)
            default => $this->handleArrayType($baseType, $pgType),
        };
    }

    /**
     * Convert PostgreSQL column definition to MariaDB
     */
    public function convertColumnDefinition(array $columnInfo): array
    {
        $pgType = $columnInfo['data_type'] ?? $columnInfo['udt_name'] ?? '';
        $length = $columnInfo['character_maximum_length'] !== null ? (int)$columnInfo['character_maximum_length'] : null;
        $precision = $columnInfo['numeric_precision'] !== null ? (int)$columnInfo['numeric_precision'] : null;
        $scale = $columnInfo['numeric_scale'] !== null ? (int)$columnInfo['numeric_scale'] : null;

        // Normalize scale: PostgreSQL's information_schema may report scale as 0 (int or string)
        // even when NUMERIC was defined without explicit scale (allowing decimals)
        // For NUMERIC/DECIMAL types, treat scale=0 as null to allow decimal places
        // Check both data_type and udt_name, and normalize the type (remove parentheses)
        $dataType = strtolower($columnInfo['data_type'] ?? '');
        $udtName = strtolower($columnInfo['udt_name'] ?? '');
        $normalizedType = strtolower(preg_replace('/\([^)]*\)/', '', $pgType));

        if (
            in_array($dataType, ['numeric', 'decimal']) ||
            in_array($udtName, ['numeric', 'decimal']) ||
            in_array($normalizedType, ['numeric', 'decimal'])
        ) {
            // If scale is 0, treat it as null (unspecified) to allow decimal places
            // This handles the case where PostgreSQL reports scale=0 even for NUMERIC(10) without explicit scale
            if ($scale === 0) {
                $scale = null; // Treat as unspecified scale, allowing decimals
            }
        }

        $isNullable = ($columnInfo['is_nullable'] ?? 'YES') === 'YES';
        $defaultValue = $columnInfo['column_default'] ?? null;
        $isAutoIncrement = str_contains(strtolower($pgType), 'serial');

        $mariadbType = $this->convertDataType($pgType, $length, $precision, $scale);

        // Handle default values
        $mariadbDefault = $this->convertDefaultValue($defaultValue, $mariadbType, $isAutoIncrement);

        return [
            'type' => $mariadbType,
            'nullable' => $isNullable,
            'default' => $mariadbDefault,
            'auto_increment' => $isAutoIncrement,
        ];
    }

    /**
     * Convert PostgreSQL default value to MariaDB
     */
    private function convertDefaultValue(?string $pgDefault, string $mariadbType, bool $isAutoIncrement): ?string
    {
        if ($pgDefault === null || $isAutoIncrement) {
            return null;
        }

        // Remove function calls and casts
        $default = trim($pgDefault);

        // Handle PostgreSQL sequence references (nextval or regclass)
        // Check for regclass first (most common sequence reference format)
        if (preg_match("/::regclass$/i", $default)) {
            // Any regclass cast is a sequence reference - handled by AUTO_INCREMENT
            return null;
        }

        // Handle nextval() function calls
        if (preg_match("/^nextval\('([^']+)'\)$/i", $default, $matches)) {
            // Sequence - handled by AUTO_INCREMENT
            return null;
        }

        if (preg_match("/^'([^']*)'::([^:]+)$/", $default, $matches)) {
            // Casted string: 'value'::type (but not regclass, handled above)
            $castType = strtolower(trim($matches[2]));
            if ($castType === 'regclass') {
                return null;
            }
            return "'" . addslashes($matches[1]) . "'";
        }

        if (preg_match("/^'([^']*)'$/", $default)) {
            // String literal
            return $default;
        }

        if (preg_match("/^now\(\)|^current_timestamp/i", $default)) {
            return 'CURRENT_TIMESTAMP';
        }

        if (preg_match("/^current_date/i", $default)) {
            return 'CURRENT_DATE';
        }

        // Numeric or boolean defaults
        if (preg_match('/^(true|false)$/i', $default)) {
            return strtoupper($default);
        }

        // Numeric values
        if (preg_match('/^-?\d+(\.\d+)?$/', $default)) {
            return $default;
        }

        // If we can't convert it, return null to avoid SQL errors
        // This is safer than including invalid PostgreSQL syntax
        return null;
    }

    private function formatDecimal(?int $precision, ?int $scale): string
    {
        // Ignore PostgreSQL precision and scale values
        // Always create DECIMAL(20, 10) - 20 total digits, 10 decimal places
        // This allows up to 10 digits on both sides of the decimal point
        // (10 integer digits + 10 decimal digits = 20 total precision, 10 scale)
        return 'DECIMAL(20, 10)';
    }

    private function formatVarchar(?int $length): string
    {
        if ($length === null || $length > 65535) {
            return 'LONGTEXT';
        }
        return "VARCHAR({$length})";
    }

    private function formatChar(?int $length): string
    {
        if ($length === null) {
            return 'CHAR(1)';
        }
        if ($length > 255) {
            return 'VARCHAR(' . min($length, 65535) . ')';
        }
        return "CHAR({$length})";
    }

    private function handleArrayType(string $baseType, string $originalType): string
    {
        // PostgreSQL array types like integer[], text[], etc.
        if (str_ends_with($originalType, '[]')) {
            // Convert to JSON
            return 'JSON';
        }

        // Unknown type - default to TEXT
        return 'LONGTEXT';
    }

    /**
     * Convert PostgreSQL index type to MariaDB
     */
    public function convertIndexType(string $pgIndexType): string
    {
        return match (strtolower($pgIndexType)) {
            'btree' => 'BTREE',
            'hash' => 'HASH',
            'gin' => 'BTREE', // GIN not supported, use BTREE
            'gist' => 'BTREE', // GiST not supported, use BTREE
            'spgist' => 'BTREE',
            'brin' => 'BTREE',
            default => 'BTREE',
        };
    }

    /**
     * Convert PostgreSQL value for insertion into MariaDB
     */
    public function convertValue(mixed $value, string $pgType): mixed
    {
        if ($value === null) {
            return null;
        }

        $baseType = strtolower(preg_replace('/\([^)]*\)/', '', $pgType));
        $baseType = trim($baseType);

        return match ($baseType) {
            'boolean', 'bool' => $this->convertBoolean($value),
            'json', 'jsonb' => $this->convertJson($value),
            'uuid' => $this->convertUuid($value),
            'bytea' => $this->convertBytea($value),
            'timestamp with time zone', 'timestamptz', 'timestamp without time zone', 'timestamp' => $this->convertTimestamp($value),
            'date' => $this->convertDate($value),
            'time without time zone', 'time' => $this->convertTime($value),
            default => $value,
        };
    }

    private function convertBoolean(mixed $value): int
    {
        if (is_bool($value)) {
            return $value ? 1 : 0;
        }
        if (is_string($value)) {
            return in_array(strtolower($value), ['true', 't', '1', 'yes', 'on']) ? 1 : 0;
        }
        return (int) (bool) $value;
    }

    private function convertJson(mixed $value): string
    {
        if (is_string($value)) {
            // Validate JSON
            json_decode($value, true);
            if (json_last_error() === JSON_ERROR_NONE) {
                return $value;
            }
        }
        return json_encode($value, JSON_UNESCAPED_UNICODE);
    }

    private function convertUuid(mixed $value): string
    {
        $uuid = (string) $value;
        // Ensure UUID format
        if (preg_match('/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i', $uuid)) {
            return $uuid;
        }
        return $uuid; // Return as-is, may need manual review
    }

    private function convertBytea(mixed $value): string
    {
        if (is_resource($value)) {
            return stream_get_contents($value);
        }
        return (string) $value;
    }

    private function convertTimestamp(mixed $value): ?string
    {
        if ($value === null) {
            return null;
        }

        $timestamp = trim((string) $value);

        // Early validation: check for corrupted years (5+ digits) before any processing
        // This catches cases like "202511-11-13 02:39:00"
        // Check at the very start of the string
        if (preg_match('/^(\d{5,})/', $timestamp)) {
            // Year has too many digits - return Unix epoch start
            return '1970-01-01 00:00:00';
        }

        // Also check specifically for the pattern: digits-dash pattern at start
        // This is more specific and catches "202511-11-13" format
        if (preg_match('/^(\d+)-/', $timestamp, $matches)) {
            $yearPart = $matches[1];
            if (strlen($yearPart) > 4) {
                return '1970-01-01 00:00:00'; // Year has too many digits
            }
        }

        // Remove timezone info if present (e.g., "+00:00" or "-05:00")
        $timestamp = preg_replace('/[+-]\d{2}:\d{2}$/', '', $timestamp);
        $timestamp = trim($timestamp);

        // Validate the date format (YYYY-MM-DD HH:MM:SS or YYYY-MM-DD)
        // Must start with exactly 4 digits for year
        if (!preg_match('/^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2}(\.\d+)?)?$/', $timestamp)) {
            // Invalid format - try to parse and reformat
            try {
                // Before parsing, check again for corrupted year
                if (preg_match('/^(\d{5,})/', $timestamp)) {
                    return '1970-01-01 00:00:00';
                }

                $date = new \DateTime($timestamp);
                $formatted = $date->format('Y-m-d H:i:s');

                // Double-check the formatted result doesn't have issues
                if (preg_match('/^(\d{5,})/', $formatted) || strlen($formatted) < 10) {
                    return '1970-01-01 00:00:00'; // Still corrupted after formatting
                }

                // Validate year in formatted result
                if (preg_match('/^(\d{4})/', $formatted, $yearMatch)) {
                    $year = (int) $yearMatch[1];
                    if ($year < 1900 || $year > 2100) {
                        return '1970-01-01 00:00:00';
                    }
                }

                return $formatted;
            } catch (\Exception $e) {
                // If parsing fails, return Unix epoch start
                return '1970-01-01 00:00:00';
            }
        }

        // Additional validation: check if year is reasonable (1900-2100)
        if (preg_match('/^(\d{4})-\d{2}-\d{2}/', $timestamp, $matches)) {
            $year = (int) $matches[1];
            if ($year < 1900 || $year > 2100) {
                return '1970-01-01 00:00:00'; // Invalid year
            }
        }

        return $timestamp;
    }

    private function convertDate(mixed $value): ?string
    {
        if ($value === null) {
            return null;
        }

        $date = (string) $value;

        // Check for corrupted dates (e.g., "202511-11-13" - year has too many digits)
        if (preg_match('/^(\d{5,})-\d{2}-\d{2}/', $date)) {
            return '1970-01-01'; // Invalid year - use Unix epoch start
        }

        // Validate date format
        if (!preg_match('/^\d{4}-\d{2}-\d{2}$/', $date)) {
            // Try to parse and reformat
            try {
                $dateObj = new \DateTime($date);
                $formatted = $dateObj->format('Y-m-d');

                // Validate year in formatted result
                if (preg_match('/^(\d{4})/', $formatted, $yearMatch)) {
                    $year = (int) $yearMatch[1];
                    if ($year < 1900 || $year > 2100) {
                        return '1970-01-01';
                    }
                }

                return $formatted;
            } catch (\Exception $e) {
                return '1970-01-01'; // Invalid date - use Unix epoch start
            }
        }

        // Validate year is reasonable (1900-2100)
        if (preg_match('/^(\d{4})-\d{2}-\d{2}$/', $date, $matches)) {
            $year = (int) $matches[1];
            if ($year < 1900 || $year > 2100) {
                return '1970-01-01'; // Invalid year - use Unix epoch start
            }
        }

        return $date;
    }

    private function convertTime(mixed $value): ?string
    {
        if ($value === null) {
            return null;
        }

        $time = (string) $value;

        // Validate time format (HH:MM:SS or HH:MM:SS.microseconds)
        if (!preg_match('/^\d{2}:\d{2}:\d{2}(\.\d+)?$/', $time)) {
            // Try to parse and reformat
            try {
                $timeObj = new \DateTime("1970-01-01 {$time}");
                return $timeObj->format('H:i:s');
            } catch (\Exception $e) {
                return null; // Invalid time
            }
        }

        return $time;
    }
}
