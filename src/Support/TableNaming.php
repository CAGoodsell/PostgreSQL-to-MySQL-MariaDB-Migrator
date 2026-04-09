<?php

declare(strict_types=1);

namespace Migration\Support;

final class TableNaming
{
    /**
     * MySQL target identifier for a given PostgreSQL table name.
     */
    public static function toTarget(string $sourceTableName, bool $lowercase): string
    {
        return $lowercase ? strtolower($sourceTableName) : $sourceTableName;
    }
}
