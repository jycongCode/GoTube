// Lab 7: Implement a SQLite video metadata service

package web

import (
	"database/sql"
	"log"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type SQLiteVideoMetadataService struct {
	targetDataBaseDir string
}

const logSQLitePrefix = "SQLiteVideoContentMetaService:"

func NewSQLiteVideoMetadataService(targetDB string) (*SQLiteVideoMetadataService, error) {
	_, err := os.Stat(targetDB)
	if os.IsExist(err) {
		log.Printf("%s Database %s already exists\n", logSQLitePrefix, targetDB)
		return &SQLiteVideoMetadataService{targetDataBaseDir: targetDB}, err
	} else {
		db, err := sql.Open("sqlite3", targetDB)
		if err != nil {
			log.Printf("%s Failed to create db file %s\n", logSQLitePrefix, targetDB)
			return nil, err
		}
		defer db.Close()
		createTableCommand := `
			CREATE TABLE IF NOT EXISTS videos(
				Id TEXT NOT NULL PRIMARY KEY,
				UploadedAt TIMESTAMP NOT NULL
			);
		`
		_, err = db.Exec(createTableCommand)
		if err != nil {
			log.Printf("%s Failed to create table for db %s\n", logSQLitePrefix, targetDB)
			os.Remove(targetDB)
			return nil, err
		}
		return &SQLiteVideoMetadataService{targetDataBaseDir: targetDB}, nil
	}
}
func (dbService *SQLiteVideoMetadataService) Read(id string) (*VideoMetadata, error) {
	_, err := os.Stat(dbService.targetDataBaseDir)
	if os.IsNotExist(err) {
		log.Printf("%s db file %s lost\n", logSQLitePrefix, dbService.targetDataBaseDir)
		return nil, err
	}
	db, err := sql.Open("sqlite3", dbService.targetDataBaseDir)
	if err != nil {
		log.Printf("%s Failed to Open db %s\n", logSQLitePrefix, dbService.targetDataBaseDir)
		return nil, err
	}
	defer db.Close()
	row := db.QueryRow("SELECT * FROM videos WHERE Id = ?", id)
	var targetId string
	var targetUploadedTime time.Time
	err = row.Scan(&targetId, &targetUploadedTime)
	if err != nil {
		log.Printf("%s Failed to get video metadata of %s", logSQLitePrefix, id)
		return nil, err
	}
	return &VideoMetadata{Id: targetId, UploadedAt: targetUploadedTime}, nil
}

func (dbService *SQLiteVideoMetadataService) List() ([]VideoMetadata, error) {
	_, err := os.Stat(dbService.targetDataBaseDir)
	if os.IsNotExist(err) {
		log.Printf("%s db file %s lost\n", logSQLitePrefix, dbService.targetDataBaseDir)
		return nil, err
	}
	db, err := sql.Open("sqlite3", dbService.targetDataBaseDir)
	if err != nil {
		log.Printf("%s Failed to Open db %s\n", logSQLitePrefix, dbService.targetDataBaseDir)
		return nil, err
	}
	defer db.Close()
	rows, err := db.Query("SELECT * FROM videos")
	if err != nil {
		log.Printf(
			"%s Failed to get videometa data from %s\n",
			logSQLitePrefix,
			dbService.targetDataBaseDir,
		)
		return nil, err
	}
	var metaData []VideoMetadata
	for rows.Next() {
		var targetId string
		var targetUploadTime time.Time
		rows.Scan(&targetId, &targetUploadTime)
		metaData = append(metaData, VideoMetadata{Id: targetId, UploadedAt: targetUploadTime})
	}
	return metaData, nil
}

func (dbService *SQLiteVideoMetadataService) Create(videoId string, uploadedAt time.Time) error {
	_, err := os.Stat(dbService.targetDataBaseDir)
	if os.IsNotExist(err) {
		log.Printf("%s db file %s lost\n", logSQLitePrefix, dbService.targetDataBaseDir)
		return nil
	}
	db, err := sql.Open("sqlite3", dbService.targetDataBaseDir)
	if err != nil {
		log.Printf("%s Failed to Open db %s\n", logSQLitePrefix, dbService.targetDataBaseDir)
		return nil
	}
	defer db.Close()
	_, err = db.Exec("INSERT INTO videos (Id,UploadedAt) VALUES (?,?)", videoId, uploadedAt)
	if err != nil {
		log.Printf("%s %s", logSQLitePrefix, err)
		return err
	}
	return nil
}

// Uncomment the following line to ensure SQLiteVideoMetadataService implements VideoMetadataService
var _ VideoMetadataService = (*SQLiteVideoMetadataService)(nil)
