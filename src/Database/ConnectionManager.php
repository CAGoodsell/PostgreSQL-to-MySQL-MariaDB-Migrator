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
    private ?SshTunnelManager $sshTunnelManager = null;
    private ?int $sourceTunnelPort = null;
    private ?int $targetTunnelPort = null;

    public function __construct(array $config)
    {
        $this->config = $config;
        $this->sshTunnelManager = new SshTunnelManager();
    }

    public function getSourceConnection(): PDO
    {
        if ($this->sourceConnection === null) {
            // Setup SSH tunnel if configured
            if (isset($this->config['source']['ssh']) && $this->config['source']['ssh']['enabled']) {
                $sshHost = $this->config['source']['ssh']['host'] ?? 'unknown';
                $sshUser = $this->config['source']['ssh']['user'] ?? 'unknown';
                $remoteHost = $this->config['source']['ssh']['remote_host'] ?? $this->config['source']['host'] ?? 'localhost';
                $remotePort = $this->config['source']['ssh']['remote_port'] ?? $this->config['source']['port'] ?? 5432;
                
                echo "[INFO] Setting up SSH tunnel for PostgreSQL source database...\n";
                echo "[INFO] SSH Server: {$sshUser}@{$sshHost}\n";
                echo "[INFO] Remote Database: {$remoteHost}:{$remotePort}\n";
                
                try {
                    $this->sourceTunnelPort = $this->setupSshTunnel($this->config['source']['ssh'], $this->config['source']);
                    echo "[SUCCESS] SSH tunnel established successfully on local port {$this->sourceTunnelPort}\n";
                } catch (\Exception $e) {
                    echo "[ERROR] SSH tunnel failed: " . $e->getMessage() . "\n";
                    throw $e;
                }
            }
            $this->sourceConnection = $this->createConnection($this->config['source'], $this->sourceTunnelPort);
        }

        return $this->sourceConnection;
    }

    public function getTargetConnection(): PDO
    {
        if ($this->targetConnection === null) {
            // Setup SSH tunnel if configured
            if (isset($this->config['target']['ssh']) && $this->config['target']['ssh']['enabled']) {
                $sshHost = $this->config['target']['ssh']['host'] ?? 'unknown';
                $sshUser = $this->config['target']['ssh']['user'] ?? 'unknown';
                $remoteHost = $this->config['target']['ssh']['remote_host'] ?? $this->config['target']['host'] ?? 'localhost';
                $remotePort = $this->config['target']['ssh']['remote_port'] ?? $this->config['target']['port'] ?? 3306;
                
                echo "[INFO] Setting up SSH tunnel for MySQL/MariaDB target database...\n";
                echo "[INFO] SSH Server: {$sshUser}@{$sshHost}\n";
                echo "[INFO] Remote Database: {$remoteHost}:{$remotePort}\n";
                
                try {
                    $this->targetTunnelPort = $this->setupSshTunnel($this->config['target']['ssh'], $this->config['target']);
                    echo "[SUCCESS] SSH tunnel established successfully on local port {$this->targetTunnelPort}\n";
                } catch (\Exception $e) {
                    echo "[ERROR] SSH tunnel failed: " . $e->getMessage() . "\n";
                    throw $e;
                }
            }
            $this->targetConnection = $this->createConnection($this->config['target'], $this->targetTunnelPort);
        }

        return $this->targetConnection;
    }

    /**
     * Setup SSH tunnel for database connection
     */
    private function setupSshTunnel(array $sshConfig, array $dbConfig): int
    {
        $sshHost = $sshConfig['host'] ?? '';
        $sshPort = (int)($sshConfig['port'] ?? 22);
        $sshUser = $sshConfig['user'] ?? '';
        $sshKeyPath = $sshConfig['key_path'] ?? null;
        $sshPassword = $sshConfig['password'] ?? null;
        $remoteHost = $sshConfig['remote_host'] ?? $dbConfig['host'] ?? 'localhost';
        $remotePort = (int)($sshConfig['remote_port'] ?? $dbConfig['port'] ?? 3306);
        $localPort = (int)($sshConfig['local_port'] ?? 0);

        if (empty($sshHost) || empty($sshUser)) {
            throw new \RuntimeException("SSH tunnel requires 'host' and 'user' to be configured");
        }

        if (empty($sshKeyPath) && empty($sshPassword)) {
            throw new \RuntimeException("SSH tunnel requires either 'key_path' or 'password' to be configured");
        }

        return $this->sshTunnelManager->createTunnel(
            $sshHost,
            $sshPort,
            $sshUser,
            $sshKeyPath,
            $sshPassword,
            $remoteHost,
            $remotePort,
            $localPort
        );
    }

    private function createConnection(array $config, ?int $tunnelPort = null): PDO
    {
        $driver = $config['driver'];
        
        // Use tunnel port if SSH tunnel is active, otherwise use configured host/port
        if ($tunnelPort !== null) {
            $host = '127.0.0.1';
            $port = $tunnelPort;
        } else {
            $host = $config['host'];
            $port = $config['port'];
        }
        
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
        // Close SSH tunnels
        if ($this->sshTunnelManager !== null) {
            $tunnelCount = 0;
            if ($this->sourceTunnelPort !== null) {
                $tunnelCount++;
            }
            if ($this->targetTunnelPort !== null) {
                $tunnelCount++;
            }
            
            if ($tunnelCount > 0) {
                echo "[INFO] Closing {$tunnelCount} SSH tunnel(s)...\n";
                $this->sshTunnelManager->closeAllTunnels();
                echo "[SUCCESS] SSH tunnels closed\n";
            }
        }
        
        $this->sourceConnection = null;
        $this->targetConnection = null;
        $this->sourceTunnelPort = null;
        $this->targetTunnelPort = null;
    }
}
