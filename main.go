package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/option"
)

const (
	chunkSize = 16 * 1024
)

func main() {
	var (
		dbUser     string
		dbPass     string
		dbHost     string
		dbPort     string
		bucketName string
		dbLimit    uint
		tableLimit uint
		skipDBs    string
	)
	flag.StringVar(&dbUser, "dbUser", "", "MySQL database username")
	flag.StringVar(&dbPass, "dbPass", "", "MySQL database password")
	flag.StringVar(&dbHost, "dbHost", "localhost", "MySQL database host")
	flag.StringVar(&dbPort, "dbPort", "3306", "MySQL database port")
	flag.StringVar(&bucketName, "bucketName", "", "GCS bucket name")
	flag.UintVar(&dbLimit, "dbLimit", 2, "DB backup concurrency limit")
	flag.UintVar(&tableLimit, "tableLimit", 2, "Table backup concurrency limit")
	flag.StringVar(&skipDBs, "skipDBs", "information_schema,performance_schema,test", "Comma-separated list of databases to skip")

	flag.Parse()

	if dbUser == "" || dbPass == "" || bucketName == "" {
		log.Fatal("Missing required command line arguments. Please provide dbUser, dbPass, and bucketName.")
	}

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("Failed to get hostname: %v", err)
	}

	databases, err := getDatabases(&dbUser, &dbPass, &dbHost, &dbPort, &skipDBs)
	if err != nil {
		log.Fatalf("Failed to retrieve list of databases: %v", err)
	}

	options := []option.ClientOption{
		option.WithScopes("https://www.googleapis.com/auth/devstorage.read_write"),
		option.WithGRPCConnectionPool(int(dbLimit * tableLimit)),
		option.WithUserAgent("mysql-backup-tables-to-gcs"),
		option.WithTelemetryDisabled(),
	}

	ctx := context.Background()
	client, err := storage.NewClient(ctx, options...)
	if err != nil {
		log.Fatalf("failed to create GCS client: %w", err)
	}
	defer client.Close()

	bucket := client.Bucket(bucketName)

	dbGroup := new(errgroup.Group)
	dbGroup.SetLimit(int(dbLimit))

	for _, database := range databases {
		database := database

		dbGroup.Go(func() error {
			log.Printf("Backing up database: %s\n", database)

			tables, err := getTables(&dbUser, &dbPass, &dbHost, &dbPort, &database)
			if err != nil {
				return fmt.Errorf("failed to retrieve list of tables for database %s: %w", database, err)
			}

			tableGroup := new(errgroup.Group)
			tableGroup.SetLimit(int(tableLimit))

			for _, table := range tables {
				table := table

				tableGroup.Go(func() error {
					backupPath := fmt.Sprintf("%s/%s/%s", hostname, time.Now().Format("2006-01-02-15"), database)

					log.Printf("Backing up table: \"%s.%s\"\n", database, table)

					cmd := exec.Command("mysqldump",
						fmt.Sprintf("--user=%s", dbUser),
						fmt.Sprintf("--password=%s", dbPass),
						fmt.Sprintf("--host=%s", dbHost),
						fmt.Sprintf("--port=%s", dbPort),
						"--routines",
						"--triggers",
						"--dump-date",
						"--quick",
						"--create-options",
						"--skip-extended-insert",
						"--hex-blob",
						"--default-character-set=utf8mb4",
						"--skip-lock-tables",
						database,
						table,
					)

					output, err := cmd.StdoutPipe()
					if err != nil {
						return fmt.Errorf("failed to create stdout pipe for mysqldump command: %w", err)
					}

					if err := cmd.Start(); err != nil {
						return fmt.Errorf("failed to start mysqldump command: %w", err)
					}

					if err := uploadToGCS(ctx, bucket, &backupPath, &table, output); err != nil {
						return fmt.Errorf("failed to upload backup for table \"%s.%s\" to GCS: %w", database, table, err)
					}

					if err := cmd.Wait(); err != nil {
						return fmt.Errorf("failed to wait for mysqldump command: %w", err)
					}

					log.Printf("Backup for table \"%s.%s\" completed.\n", database, table)

					return nil
				})
			}

			if err := tableGroup.Wait(); err != nil {
				log.Println(err)
				return err
			}

			log.Printf("Backup for database %s completed.\n", database)

			return nil
		})
	}

	if err := dbGroup.Wait(); err != nil {
		log.Fatalf("Database backup failed: %v", err)
	}

	log.Println("Database backup completed")
}

func getDatabases(dbUser *string, dbPass *string, dbHost *string, dbPort *string, skipDBs *string) ([]string, error) {
	cmd := exec.Command("mysql",
		"--user="+*dbUser,
		"--password="+*dbPass,
		"--host="+*dbHost,
		"--port="+*dbPort,
		"--skip-column-names",
		"-e", "SHOW DATABASES",
	)

	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to execute mysql command: %w", err)
	}

	var databases []string
	skipDBList := strings.Split(*skipDBs, ",")

	scanner := bufio.NewScanner(strings.NewReader(string(output)))
	for scanner.Scan() {
		database := scanner.Text()
		if !contains(&skipDBList, &database) {
			databases = append(databases, database)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read output from mysql command: %w", err)
	}

	return databases, nil
}

func getTables(dbUser *string, dbPass *string, dbHost *string, dbPort *string, database *string) ([]string, error) {
	cmd := exec.Command("mysql",
		"--user="+*dbUser,
		"--password="+*dbPass,
		"--host="+*dbHost,
		"--port="+*dbPort,
		"--skip-column-names",
		"-e", fmt.Sprintf("SHOW TABLES FROM `%s`", *database),
	)

	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to execute mysql command: %w", err)
	}

	var tables []string
	scanner := bufio.NewScanner(strings.NewReader(string(output)))
	for scanner.Scan() {
		table := scanner.Text()
		tables = append(tables, table)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read output from mysql command: %w", err)
	}

	return tables, nil
}

func uploadToGCS(ctx context.Context, bucket *storage.BucketHandle, backupPath *string, table *string, reader io.Reader) error {
	object := bucket.Object(fmt.Sprintf("%s/%s.sql.gz", *backupPath, *table))
	writer := object.NewWriter(ctx)
	gzipWriter := gzip.NewWriter(writer)
	bufWriter := bufio.NewWriterSize(gzipWriter, chunkSize)

	if _, err := io.Copy(bufWriter, reader); err != nil {
		return fmt.Errorf("failed to upload backup for table \"%s\" to GCS: %w", *table, err)
	}

	if err := bufWriter.Flush(); err != nil {
		return fmt.Errorf("failed to close bufWriter: %w", err)
	}

	if err := gzipWriter.Close(); err != nil {
		return fmt.Errorf("failed to close gzipWriter: %w", err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}

	if _, err := object.Attrs(ctx); err != nil {
		return fmt.Errorf("failed to retrieve attributes for GCS object: %w", err)
	}

	return nil
}

func contains(slice *[]string, value *string) bool {
	for _, item := range *slice {
		if item == *value {
			return true
		}
	}
	return false
}
