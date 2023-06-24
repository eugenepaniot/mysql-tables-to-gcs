all:
	go mod tidy
	go build -o mysql-backup-tables-to-gcs .
