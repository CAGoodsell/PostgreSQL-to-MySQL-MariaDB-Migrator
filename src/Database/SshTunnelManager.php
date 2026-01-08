<?php

declare(strict_types=1);

namespace Migration\Database;

use RuntimeException;

class SshTunnelManager
{
    private array $tunnels = [];
    private array $processes = [];

    /**
     * Create an SSH tunnel for database connection
     *
     * @param string $sshHost SSH server hostname or IP
     * @param int $sshPort SSH server port (default: 22)
     * @param string $sshUser SSH username
     * @param string $sshKeyPath Path to SSH private key file (optional, uses password auth if not provided)
     * @param string $sshPassword SSH password (optional, uses key auth if not provided)
     * @param string $remoteHost Remote database host (usually localhost or 127.0.0.1)
     * @param int $remotePort Remote database port
     * @param int $localPort Local port to forward to (0 = auto-assign)
     * @return int The local port number
     * @throws RuntimeException
     */
    public function createTunnel(
        string $sshHost,
        int $sshPort,
        string $sshUser,
        ?string $sshKeyPath,
        ?string $sshPassword,
        string $remoteHost,
        int $remotePort,
        int $localPort = 0
    ): int {
        // Generate a unique tunnel ID
        $tunnelId = md5("{$sshHost}:{$sshPort}:{$sshUser}:{$remoteHost}:{$remotePort}");

        // Check if tunnel already exists
        if (isset($this->tunnels[$tunnelId])) {
            return $this->tunnels[$tunnelId]['local_port'];
        }

        // Find an available local port if not specified
        if ($localPort === 0) {
            $localPort = $this->findAvailablePort();
        }

        // Build SSH command
        $sshCommand = $this->buildSshCommand(
            $sshHost,
            $sshPort,
            $sshUser,
            $sshKeyPath,
            $sshPassword,
            $remoteHost,
            $remotePort,
            $localPort
        );

        // Start SSH tunnel process
        $descriptorspec = [
            0 => ['pipe', 'r'],  // stdin
            1 => ['pipe', 'w'],  // stdout
            2 => ['pipe', 'w'],  // stderr
        ];

        $process = proc_open($sshCommand, $descriptorspec, $pipes);

        if (!is_resource($process)) {
            throw new RuntimeException("Failed to start SSH tunnel process");
        }

        // Store process info
        $this->processes[$tunnelId] = [
            'process' => $process,
            'pipes' => $pipes,
        ];

        // Wait a moment for tunnel to establish
        usleep(500000); // 0.5 seconds

        // Check if process is still running
        $status = proc_get_status($process);
        if (!$status['running']) {
            $error = stream_get_contents($pipes[2]);
            fclose($pipes[0]);
            fclose($pipes[1]);
            fclose($pipes[2]);
            proc_close($process);
            throw new RuntimeException("SSH tunnel failed to start: " . ($error ?: 'Unknown error'));
        }

        // Test if local port is listening
        if (!$this->isPortListening($localPort)) {
            $error = stream_get_contents($pipes[2]);
            fclose($pipes[0]);
            fclose($pipes[1]);
            fclose($pipes[2]);
            proc_close($process);
            throw new RuntimeException("SSH tunnel port {$localPort} is not listening: " . ($error ?: 'Unknown error'));
        }

        // Store tunnel info
        $this->tunnels[$tunnelId] = [
            'local_port' => $localPort,
            'remote_host' => $remoteHost,
            'remote_port' => $remotePort,
            'ssh_host' => $sshHost,
            'ssh_port' => $sshPort,
            'ssh_user' => $sshUser,
        ];

        return $localPort;
    }

    /**
     * Build SSH command for port forwarding
     */
    private function buildSshCommand(
        string $sshHost,
        int $sshPort,
        string $sshUser,
        ?string $sshKeyPath,
        ?string $sshPassword,
        string $remoteHost,
        int $remotePort,
        int $localPort
    ): string {
        $sshPath = $this->findSshCommand();
        
        // Base SSH command with options
        $options = [
            '-N', // Don't execute remote command
            '-f', // Background
            '-o', 'StrictHostKeyChecking=no', // Don't prompt for host key verification
            '-o', 'LogLevel=ERROR', // Reduce log output
            '-L', "{$localPort}:{$remoteHost}:{$remotePort}", // Local port forwarding
            '-p', (string)$sshPort,
        ];
        
        // UserKnownHostsFile option (Windows uses nul instead of /dev/null)
        if (strtoupper(substr(PHP_OS, 0, 3)) === 'WIN') {
            $options[] = '-o';
            $options[] = 'UserKnownHostsFile=nul';
        } else {
            $options[] = '-o';
            $options[] = 'UserKnownHostsFile=/dev/null';
        }

        // Add SSH key if provided
        if ($sshKeyPath && file_exists($sshKeyPath)) {
            $options[] = '-i';
            $options[] = escapeshellarg($sshKeyPath);
        }

        // Add password via sshpass if provided (requires sshpass to be installed)
        $command = '';
        if ($sshPassword && !$sshKeyPath) {
            $sshpassPath = $this->findSshpassCommand();
            if ($sshpassPath) {
                // Use -p flag to pass password directly (less secure but simpler)
                $command = escapeshellcmd($sshpassPath) . ' -p ' . escapeshellarg($sshPassword) . ' ';
            } else {
                $isWindows = strtoupper(substr(PHP_OS, 0, 3)) === 'WIN';
                $errorMsg = "SSH password authentication requires 'sshpass' to be installed.\n";
                
                if ($isWindows) {
                    $errorMsg .= "\n";
                    $errorMsg .= "Windows does not include 'sshpass' by default. Options:\n";
                    $errorMsg .= "1. Use SSH key authentication instead (recommended):\n";
                    $errorMsg .= "   - Set PG_SSH_KEY_PATH or MYSQL_SSH_KEY_PATH in your .env file\n";
                    $errorMsg .= "   - Generate an SSH key pair if needed: ssh-keygen -t rsa -b 4096\n";
                    $errorMsg .= "   - Copy your public key to the server: ssh-copy-id user@host\n";
                    $errorMsg .= "\n";
                    $errorMsg .= "2. Install sshpass for Windows (not recommended):\n";
                    $errorMsg .= "   - Download from: https://github.com/keimpx/sshpass-windows\n";
                    $errorMsg .= "   - Add to your system PATH\n";
                } else {
                    $errorMsg .= "\n";
                    $errorMsg .= "Install sshpass:\n";
                    $errorMsg .= "  - Ubuntu/Debian: sudo apt-get install sshpass\n";
                    $errorMsg .= "  - CentOS/RHEL: sudo yum install sshpass\n";
                    $errorMsg .= "  - macOS: brew install hudochenkov/sshpass/sshpass\n";
                    $errorMsg .= "\n";
                    $errorMsg .= "Or use SSH key authentication instead (recommended):\n";
                    $errorMsg .= "  - Set PG_SSH_KEY_PATH or MYSQL_SSH_KEY_PATH in your .env file\n";
                }
                
                throw new RuntimeException($errorMsg);
            }
        }

        $command .= escapeshellcmd($sshPath) . ' ' . implode(' ', array_map('escapeshellarg', $options));
        $command .= ' ' . escapeshellarg("{$sshUser}@{$sshHost}");

        return $command;
    }

    /**
     * Find SSH command path
     */
    private function findSshCommand(): string
    {
        // On Windows, try common SSH locations
        if (strtoupper(substr(PHP_OS, 0, 3)) === 'WIN') {
            $paths = [
                'ssh', // Should be in PATH if OpenSSH is installed
                'C:\\Windows\\System32\\OpenSSH\\ssh.exe',
                'C:\\Program Files\\OpenSSH\\ssh.exe',
            ];
        } else {
            $paths = ['ssh', '/usr/bin/ssh', '/usr/local/bin/ssh'];
        }
        
        foreach ($paths as $path) {
            if ($this->commandExists($path)) {
                return $path;
            }
        }

        throw new RuntimeException(
            "SSH command not found. Please ensure SSH is installed and available in PATH. " .
            "On Windows, install OpenSSH Client (Settings > Apps > Optional Features > OpenSSH Client)."
        );
    }

    /**
     * Find sshpass command path
     */
    private function findSshpassCommand(): ?string
    {
        $paths = ['sshpass', '/usr/bin/sshpass', '/usr/local/bin/sshpass'];
        
        foreach ($paths as $path) {
            if ($this->commandExists($path)) {
                return $path;
            }
        }

        return null;
    }

    /**
     * Check if command exists
     */
    private function commandExists(string $command): bool
    {
        $where = strtoupper(substr(PHP_OS, 0, 3)) === 'WIN' ? 'where' : 'which';
        $output = [];
        $returnVar = 0;
        exec("{$where} {$command} 2>&1", $output, $returnVar);
        return $returnVar === 0;
    }

    /**
     * Find an available local port
     */
    private function findAvailablePort(int $startPort = 33000): int
    {
        for ($port = $startPort; $port < $startPort + 1000; $port++) {
            if (!$this->isPortListening($port)) {
                return $port;
            }
        }

        throw new RuntimeException("Could not find an available local port");
    }

    /**
     * Check if a port is listening
     */
    private function isPortListening(int $port): bool
    {
        $connection = @fsockopen('127.0.0.1', $port, $errno, $errstr, 0.1);
        if ($connection) {
            fclose($connection);
            return true;
        }
        return false;
    }

    /**
     * Close all SSH tunnels
     */
    public function closeAllTunnels(): void
    {
        foreach ($this->processes as $tunnelId => $processInfo) {
            $this->closeTunnel($tunnelId);
        }
    }

    /**
     * Close a specific tunnel
     */
    public function closeTunnel(string $tunnelId): void
    {
        if (!isset($this->processes[$tunnelId])) {
            return;
        }

        $processInfo = $this->processes[$tunnelId];
        $process = $processInfo['process'];
        $pipes = $processInfo['pipes'];

        // Close pipes
        foreach ($pipes as $pipe) {
            if (is_resource($pipe)) {
                fclose($pipe);
            }
        }

        // Terminate process
        if (is_resource($process)) {
            proc_terminate($process);
            proc_close($process);
        }

        unset($this->processes[$tunnelId]);
        unset($this->tunnels[$tunnelId]);
    }

    /**
     * Get tunnel info
     */
    public function getTunnelInfo(string $tunnelId): ?array
    {
        return $this->tunnels[$tunnelId] ?? null;
    }

    /**
     * Cleanup on destruct
     */
    public function __destruct()
    {
        $this->closeAllTunnels();
    }
}
