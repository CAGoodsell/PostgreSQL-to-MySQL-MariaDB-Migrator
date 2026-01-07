<?php

declare(strict_types=1);

namespace Migration\Database;

use PDO;
use PDOException;

class ConnectionManager
{
    private ?PDO $sourceConnection = null;
    private ?PDO $targetConnection = null;
    private array $config;

    public function __construct(array $config)
    {
        $this->config = $config;
    }

    public function getSourceConnection(): PDO
    {
        if ($this->sourceConnection === null) {
            $this->sourceConnection = $this->createConnection($this->config['source']);
        }

        return $this->sourceConnection;
    }

    public function getTargetConnection(): PDO
    {
        if ($this->targetConnection === null) {
            $this->targetConnection = $this->createConnection($this->config['target']);
        }

        return $this->targetConnection;
    }

    private function createConnection(array $config): PDO
    {
        $driver = $config['driver'];
        $host = $config['host'];
        $port = $config['port'];
        $database = $config['database'];
        $username = $config['username'];
        $password = $config['password'];
        $charset = $config['charset'] ?? 'utf8';

        $dsn = match ($driver) {
            'pgsql' => "pgsql:host={$host};port={$port};dbname={$database}",
            'mysql' => "mysql:host={$host};port={$port};dbname={$database};charset={$charset}",
            default => throw new \InvalidArgumentException("Unsupported driver: {$driver}"),
        };

        try {
            $pdo = new PDO($dsn, $username, $password, [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
                PDO::ATTR_EMULATE_PREPARES => false,
            ]);

            if ($driver === 'mysql') {
                $pdo->exec("SET NAMES {$charset}");
            }

            return $pdo;
        } catch (PDOException $e) {
            throw new \RuntimeException(
                "Failed to connect to {$driver} database: " . $e->getMessage(),
                0,
                $e
            );
        }
    }

    public function testConnections(): bool
    {
        try {
            $this->getSourceConnection()->query('SELECT 1');
            $this->getTargetConnection()->query('SELECT 1');
            return true;
        } catch (\Exception $e) {
            throw new \RuntimeException("Connection test failed: " . $e->getMessage(), 0, $e);
        }
    }

    public function closeConnections(): void
    {
        $this->sourceConnection = null;
        $this->targetConnection = null;
    }
}
