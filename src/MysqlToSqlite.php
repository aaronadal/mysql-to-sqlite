<?php

namespace Aaronadal\MysqlToSqlite;


/**
 * @author Aarón Nadal <aaron@planatec.es>
 */
class MysqlToSqlite
{

    /**
     * @var string
     */
    private $mysqldumpExecutable;

    /**
     * @var string
     */
    private $mysqlHost;

    /**
     * @var string
     */
    private $mysqlPort;

    /**
     * @var string
     */
    private $mysqlUser;

    /**
     * @var string
     */
    private $mysqlPassword;

    /**
     * @var string
     */
    private $mysqlDatabaseName;

    /**
     * @var string
     */
    private $mysqlDumpFilePath;

    /**
     * @var string
     */
    private $sqliteExecutable;

    /**
     * Creates a new MysqlToSqlite instance.
     *
     * @param string $mysqldumpExecutable
     * @param string $mysqlHost
     * @param string $mysqlPort
     * @param string $mysqlUser
     * @param string $mysqlPassword
     * @param string $mysqlDatabaseName
     * @param string $mysqlDumpFilePath
     * @param string $sqliteExecutable
     */
    public function __construct(string $mysqldumpExecutable, string $mysqlHost, string $mysqlPort, string $mysqlUser, string $mysqlPassword, string $mysqlDatabaseName, string $mysqlDumpFilePath, string $sqliteExecutable)
    {
        $this->mysqldumpExecutable = $mysqldumpExecutable;
        $this->mysqlHost           = $mysqlHost;
        $this->mysqlPort           = $mysqlPort;
        $this->mysqlUser           = $mysqlUser;
        $this->mysqlPassword       = $mysqlPassword;
        $this->mysqlDatabaseName   = $mysqlDatabaseName;
        $this->mysqlDumpFilePath   = $mysqlDumpFilePath;
        $this->sqliteExecutable    = $sqliteExecutable;
    }

    /**
     * Performs the conversion.
     *
     * @param string $targetFilePath
     */
    public function convert(string $targetFilePath): void
    {
        $exportCommand = $this->getExportCommand($this->mysqlDumpFilePath);
        $importCommand = $this->getImportCommand($targetFilePath);

        exec($exportCommand);

        $this->sanitizeMysqlDump($this->mysqlDumpFilePath);

        $this->backupPreviousSqliteDatabase($targetFilePath);

        exec($importCommand);
    }

    private function getExportCommand(string $mysqlDumpFilePath): string
    {
        $exportCommand = "\"%s\" --skip-create-options --compatible=ansi --skip-extended-insert --compact --single-transaction -h %s -P %s -u %s -p%s %s > %s";

        $exportCommand = sprintf(
            $exportCommand,
            $this->mysqldumpExecutable,
            $this->mysqlHost,
            $this->mysqlPort,
            $this->mysqlUser,
            $this->mysqlPassword,
            $this->mysqlDatabaseName,
            $mysqlDumpFilePath
        );

        return $exportCommand;
    }

    private function getImportCommand(string $targetFilePath): string
    {
        $importCommand = "\"%s\" %s < %s";

        $importCommand = sprintf(
            $importCommand,
            $this->sqliteExecutable,
            $targetFilePath,
            $this->mysqlDumpFilePath
        );

        return $importCommand;
    }

    /**
     * Sanitizes the dump in order to make it compatible with Sqlite.
     * It does the next steps:
     *
     * - Removes lines containing "KEY".
     * - Removes trailing commas before closing parentheses.
     * - Removes COLLATE utfmb4_unicode_ci.
     * - Removes CHARACTER SET utf8.
     * - Removes " unsigned".
     * - Replaces \' to ''.
     *
     * @param string $mysqlDumpFilePath
     */
    private function sanitizeMysqlDump(string $mysqlDumpFilePath): void
    {
        $dump    = file_get_contents($mysqlDumpFilePath);
        $dump    = mb_convert_encoding($dump, 'UTF-8', 'windows-1252');
        $lines   = explode("\n", $dump);

        $includedLines = [];
        foreach ($lines as $line) {
            $line = trim($line);
            if (strpos($line, 'KEY') === false) {
                if (strpos($line, ')') === 0) {
                    $lastIndex           = count($includedLines) - 1;
                    $includedLines[$lastIndex] = rtrim($includedLines[$lastIndex], ',');
                }

                $line = preg_replace('/COLLATE\s.*?(\s|$)/', '', $line);
                $line = preg_replace('/CHARACTER\sSET\s.*?(\s|$)/', '', $line);
                $line = str_replace(' unsigned', '', $line);
                $line = str_replace("\\'", "''", $line);

                $includedLines[] = $line;
            }
        }

        file_put_contents($mysqlDumpFilePath, implode("\n", $includedLines));
    }

    /**
     * Backups the previous Sqlite database, if exists.
     *
     * @param string $targetFilePath
     */
    private function backupPreviousSqliteDatabase(string $targetFilePath): void
    {
        if (file_exists($targetFilePath)) {
            copy($targetFilePath, $targetFilePath . '.bk');
            unlink($targetFilePath);
        }
    }
}
