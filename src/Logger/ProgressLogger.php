<?php

declare(strict_types=1);

namespace Migration\Logger;

class ProgressLogger
{
    private string $logFile;
    private string $checkpointDir;
    private $logHandle;
    private array $checkpoints = [];
    private array $tableStartTimes = [];
    private ?float $overallStartTime = null;

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

    public function progress(
        string $table,
        int $processed,
        int $total,
        float $percentage,
        ?int $overallProcessed = null,
        ?int $overallTotal = null,
        ?float $overallPercentage = null
    ): void
    {
        // Track start time for this table if not already set
        if (!isset($this->tableStartTimes[$table])) {
            $this->tableStartTimes[$table] = microtime(true);
        }

        // Keep the console progress line compact to avoid wrapping.
        $barLength = 30;
        $filled = (int) ($barLength * $percentage / 100);
        $bar = str_repeat('=', $filled) . str_repeat(' ', $barLength - $filled);
        
        // Calculate estimated time to completion
        $eta = $this->calculateETA($table, $processed, $total, $percentage);

        // Full detail message (logged to file)
        $fileMessage = sprintf(
            "%s: [%s] %d/%d (%.2f%%) %s",
            $table,
            $bar,
            $processed,
            $total,
            $percentage,
            $eta
        );

        // Console message (compact)
        $tableLabel = $table;
        if (strlen($tableLabel) > 18) {
            $tableLabel = substr($tableLabel, 0, 18);
        }
        $consoleMessage = sprintf(
            "%s [%s] %.2f%% %s",
            $tableLabel,
            $bar,
            $percentage,
            $eta
        );

        // Optional overall migration progress + ETA (shown on same line)
        if ($overallProcessed !== null && $overallTotal !== null && $overallPercentage !== null && $overallTotal > 0) {
            if ($this->overallStartTime === null) {
                $this->overallStartTime = microtime(true);
            }

            $overallEta = $this->calculateOverallETA($overallProcessed, $overallTotal, $overallPercentage);

            $overallBarLength = 15;
            $overallFilled = (int) ($overallBarLength * $overallPercentage / 100);
            $overallBar = str_repeat('=', $overallFilled) . str_repeat(' ', $overallBarLength - $overallFilled);

            $fileMessage .= sprintf(
                " | Total: [%s] %d/%d (%.2f%%) %s",
                $overallBar,
                $overallProcessed,
                $overallTotal,
                $overallPercentage,
                $overallEta
            );

            $consoleMessage .= sprintf(
                " | Total [%s] %.2f%% %s",
                $overallBar,
                $overallPercentage,
                $overallEta
            );
        }
        
        // Output to console with carriage return to overwrite same line.
        // IMPORTANT: If the line is wider than the terminal, it will wrap and appear as "new lines".
        // So we truncate/pad to the current terminal width.
        $width = $this->getTerminalWidth();
        $rendered = $consoleMessage;
        if (strlen($rendered) >= $width) {
            // Leave at least 1 char for safety; no ellipsis to keep it stable.
            $rendered = substr($rendered, 0, max(1, $width - 1));
        }
        $pad = max(0, $width - strlen($rendered));
        echo "\r" . $rendered . str_repeat(' ', $pad) . "\033[K"; // \033[K clears to end of line
        flush();
        
        // Also log to file (with newline for file)
        $timestamp = date('Y-m-d H:i:s');
        $logMessage = "[{$timestamp}] [PROGRESS] {$fileMessage}\n";
        fwrite($this->logHandle, $logMessage);
        
        // Clear start time when table is complete
        if ($percentage >= 100) {
            unset($this->tableStartTimes[$table]);
        }
    }

    private function getTerminalWidth(): int
    {
        // Common env vars set by many terminals (including CI). On Windows these may be absent.
        $columns = getenv('COLUMNS');
        if ($columns === false && isset($_SERVER['COLUMNS'])) {
            $columns = (string) $_SERVER['COLUMNS'];
        }

        $width = is_string($columns) ? (int) $columns : 0;
        // Safe default: keep conservative to avoid wrapping.
        if ($width <= 0) {
            return 120;
        }

        // Avoid tiny/absurd values
        return max(60, min(400, $width));
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

    /**
     * Calculate estimated time to completion
     */
    private function calculateETA(string $table, int $processed, int $total, float $percentage): string
    {
        // If already complete or no progress, don't show ETA
        if ($processed <= 0 || $percentage >= 100) {
            return '';
        }

        $startTime = $this->tableStartTimes[$table];
        $elapsedTime = microtime(true) - $startTime;
        
        // Need at least 1 second of data for meaningful estimate
        if ($elapsedTime < 1.0) {
            return 'ETA: calculating...';
        }

        // Calculate rate (rows per second)
        $rowsPerSecond = $processed / $elapsedTime;
        
        // Calculate remaining rows
        $remainingRows = $total - $processed;
        
        // Calculate estimated seconds remaining
        $estimatedSeconds = $remainingRows / $rowsPerSecond;
        
        // Format time in human-readable format
        return 'ETA: ' . $this->formatTime($estimatedSeconds);
    }

    /**
     * Calculate overall ETA for full migration progress.
     */
    private function calculateOverallETA(int $processed, int $total, float $percentage): string
    {
        if ($processed <= 0 || $percentage >= 100 || $this->overallStartTime === null) {
            return '';
        }

        $elapsedTime = microtime(true) - $this->overallStartTime;
        if ($elapsedTime < 1.0) {
            return 'ETA: calculating...';
        }

        $rowsPerSecond = $processed / $elapsedTime;
        if ($rowsPerSecond <= 0) {
            return 'ETA: calculating...';
        }

        $remainingRows = $total - $processed;
        if ($remainingRows <= 0) {
            return '';
        }

        $estimatedSeconds = $remainingRows / $rowsPerSecond;
        return 'ETA: ' . $this->formatTime($estimatedSeconds);
    }

    /**
     * Format seconds into human-readable time string
     */
    private function formatTime(float $seconds): string
    {
        // Round up so ETA never underestimates, and use integer math to avoid
        // float-to-int precision loss warnings (PHP 8.1+).
        if (!is_finite($seconds) || $seconds <= 0) {
            return '0s';
        }

        $totalSeconds = (int) ceil($seconds);

        if ($totalSeconds < 60) {
            return sprintf('%ds', $totalSeconds); 
        }
        
        $minutes = intdiv($totalSeconds, 60);
        $remainingSeconds = $totalSeconds % 60;
        
        if ($minutes < 60) {
            if ($remainingSeconds > 0) {
                return sprintf('%dm %ds', $minutes, $remainingSeconds);
            }
            return sprintf('%dm', $minutes);
        }
        
        $hours = intdiv($minutes, 60);
        $remainingMinutes = $minutes % 60;
        
        if ($remainingMinutes > 0) {
            return sprintf('%dh %dm', $hours, $remainingMinutes);
        }
        return sprintf('%dh', $hours);
    }

    public function __destruct()
    {
        if (is_resource($this->logHandle)) {
            fclose($this->logHandle);
        }
    }
}
