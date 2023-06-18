# Mysql Database Backup Tool

The Mysql Database Backup Tool is a command-line application written in Go that allows you to backup MySQL databases and store the backups directly to Google Cloud Storage. It supports concurrent backup of multiple databases and tables, improving backup efficiency and performance.

## Features

- Backup multiple databases concurrently
- Backup multiple tables within each database concurrently
- Upload backups directly to Google Cloud Storage
- Configurable concurrency limits for database and table backups

## Usage

The tool accepts several command-line arguments to configure the backup process:

```shell
./mysql-backup-tables-to-gcs -dbUser=<MySQL username> -dbPass=<MySQL password> -bucketName=<Google Cloud Storage bucket> [options]
```

Command-line options:

* `-dbUser`: MySQL database username (required)
* `-dbPass`: MySQL database password (required)
* `-dbHost`: MySQL database host (default: localhost)
* `-dbPort`: MySQL database port (default: 3306)
* `-bucketName`: Google Cloud Storage bucket name (required)
* `-dbLimit`: Database backup concurrency limit (default: 2)
* `-tableLimit`: Table backup concurrency limit (default: 2)
