package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/sync/errgroup"
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
	flag.StringVar(&bucketName, "bucketName", "", "Google Cloud Storage bucket name")
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

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/", dbUser, dbPass, dbHost, dbPort))
	if err != nil {
		log.Fatalf("Failed to connect to MySQL: %v", err)
	}
	defer db.Close()

	databases, err := getDatabases(db, skipDBs)
	if err != nil {
		log.Fatalf("Failed to retrieve list of databases: %v", err)
	}

	ctx := context.Background()
	dbGroup, ctx := errgroup.WithContext(ctx)
	dbGroup.SetLimit(int(dbLimit))

	for _, database := range databases {
		database := database

		dbGroup.Go(func() error {
			log.Printf("Backing up database: %s\n", database)

			tables, err := getTables(db, database)
			if err != nil {
				return fmt.Errorf("failed to retrieve list of tables for database %s: %w", database, err)
			}

			tableGroup, _ := errgroup.WithContext(ctx)
			tableGroup.SetLimit(int(tableLimit))

			for _, table := range tables {
				table := table

				tableGroup.Go(func() error {
					backupPath := fmt.Sprintf("%s/%s/%s", hostname, database, time.Now().Format("2006-01-02-15"))

					log.Printf("Backing up table: %s\n", table)

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

					if err := uploadToGCS(ctx, bucketName, backupPath, table, output); err != nil {
						return fmt.Errorf("failed to upload backup for table %s to Google Cloud Storage: %w", table, err)
					}

					if err := cmd.Wait(); err != nil {
						return fmt.Errorf("failed to wait for mysqldump command: %w", err)
					}

					log.Printf("Backup for table %s completed.\n", table)

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

func getDatabases(db *sql.DB, skipDBs string) ([]string, error) {
	rows, err := db.Query("SHOW DATABASES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var databases []string
	skipDBList := strings.Split(skipDBs, ",")
	for rows.Next() {
		var database string
		if err := rows.Scan(&database); err != nil {
			return nil, err
		}
		if !contains(skipDBList, database) {
			databases = append(databases, database)
		}
	}

	return databases, nil

}

func getTables(db *sql.DB, database string) ([]string, error) {
	rows, err := db.Query(fmt.Sprintf("SHOW TABLES FROM `%s`", database))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}

	return tables, nil
}

func uploadToGCS(ctx context.Context, bucketName, backupPath, table string, reader io.Reader) error {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create Google Cloud Storage client: %w", err)
	}
	defer client.Close()

	bucket := client.Bucket(bucketName)
	object := bucket.Object(fmt.Sprintf("%s/%s.sql.gz", backupPath, table))

	writer := object.NewWriter(ctx)
	defer writer.Close()

	gzipWriter := gzip.NewWriter(writer)
	defer gzipWriter.Close()

	bufWriter := bufio.NewWriterSize(gzipWriter, chunkSize)

	if _, err := io.Copy(bufWriter, reader); err != nil {
		return fmt.Errorf("failed to upload backup for table %s to Google Cloud Storage: %w", table, err)
	}

	if err := bufWriter.Flush(); err != nil {
		return fmt.Errorf("failed to flush data to Google Cloud Storage: %w", err)
	}

	return nil
}

func contains(slice []string, value string) bool {
	for _, item := range slice {
		if item == value {
			return true
		}
	}
	return false
}
