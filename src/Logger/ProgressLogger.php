<?php

declare(strict_types=1);

namespace Migration\Logger;

class ProgressLogger
{
    private string $logFile;
    private string $checkpointDir;
    private $logHandle;
    private array $checkpoints = [];

    public function __construct(string $logDir, string $checkpointDir)
    {
        $this->logFile = $logDir . '/migration_' . date('Y-m-d_His') . '.log';
        $this->checkpointDir = $checkpointDir;

        // Ensure directories exist
        if (!is_dir($logDir)) {
            mkdir($logDir, 0755, true);
        }
        if (!is_dir($checkpointDir)) {
            mkdir($checkpointDir, 0755, true);
        }

        $this->logHandle = fopen($this->logFile, 'a');
        if ($this->logHandle === false) {
            throw new \RuntimeException("Failed to open log file: {$this->logFile}");
        }
    }

    public function log(string $message, string $level = 'INFO'): void
    {
        $timestamp = date('Y-m-d H:i:s');
        $logMessage = "[{$timestamp}] [{$level}] {$message}\n";
        
        fwrite($this->logHandle, $logMessage);
        echo $logMessage;
    }

    public function info(string $message): void
    {
        $this->log($message, 'INFO');
    }

    public function warning(string $message): void
    {
        $this->log($message, 'WARNING');
    }

    public function error(string $message): void
    {
        $this->log($message, 'ERROR');
    }

    public function success(string $message): void
    {
        $this->log($message, 'SUCCESS');
    }

    public function progress(string $table, int $processed, int $total, float $percentage): void
    {
        $barLength = 50;
        $filled = (int) ($barLength * $percentage / 100);
        $bar = str_repeat('=', $filled) . str_repeat(' ', $barLength - $filled);
        
        $message = sprintf(
            "%s: [%s] %d/%d (%.2f%%)",
            $table,
            $bar,
            $processed,
            $total,
            $percentage
        );
        
        // Output to console with carriage return to overwrite same line
        echo "\r" . str_pad($message, 120) . "\033[K"; // \033[K clears to end of line
        flush();
        
        // Also log to file (with newline for file)
        $timestamp = date('Y-m-d H:i:s');
        $logMessage = "[{$timestamp}] [PROGRESS] {$message}\n";
        fwrite($this->logHandle, $logMessage);
    }

    public function saveCheckpoint(string $table, array $data): void
    {
        $checkpointFile = $this->checkpointDir . '/' . $table . '_checkpoint.json';
        $this->checkpoints[$table] = $data;
        
        file_put_contents(
            $checkpointFile,
            json_encode($data, JSON_PRETTY_PRINT),
            LOCK_EX
        );
    }

    public function loadCheckpoint(string $table): ?array
    {
        if (isset($this->checkpoints[$table])) {
            return $this->checkpoints[$table];
        }

        $checkpointFile = $this->checkpointDir . '/' . $table . '_checkpoint.json';
        
        if (!file_exists($checkpointFile)) {
            return null;
        }

        $data = json_decode(file_get_contents($checkpointFile), true);
        $this->checkpoints[$table] = $data;
        
        return $data;
    }

    public function clearCheckpoint(string $table): void
    {
        $checkpointFile = $this->checkpointDir . '/' . $table . '_checkpoint.json';
        
        if (file_exists($checkpointFile)) {
            unlink($checkpointFile);
        }
        
        unset($this->checkpoints[$table]);
    }

    public function getAllCheckpoints(): array
    {
        $checkpoints = [];
        $files = glob($this->checkpointDir . '/*_checkpoint.json');
        
        foreach ($files as $file) {
            $table = basename($file, '_checkpoint.json');
            $checkpoints[$table] = json_decode(file_get_contents($file), true);
        }
        
        return $checkpoints;
    }

    public function clearAllCheckpoints(): void
    {
        $files = glob($this->checkpointDir . '/*_checkpoint.json');
        
        foreach ($files as $file) {
            unlink($file);
        }
        
        $this->checkpoints = [];
    }

    public function getLogFile(): string
    {
        return $this->logFile;
    }

    public function __destruct()
    {
        if (is_resource($this->logHandle)) {
            fclose($this->logHandle);
        }
    }
}
