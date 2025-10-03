// Lab 7: Implement a local filesystem video content service

package web

import (
	"io"
	"log"
	"os"
	"path/filepath"
)

// FSVideoContentService implements VideoContentService using the local filesystem.
type FSVideoContentService struct {
	storageDir string
}

const logFSPrefix = "FSVideoContentService:"

func NewFSVideoContentService(targetDir string) (*FSVideoContentService, error) {
	err := os.Mkdir(targetDir, 0755)
	if err != nil {
		if os.IsExist(err) {
			log.Printf("%s Storage Dir %s already exists", logFSPrefix, targetDir)
			return &FSVideoContentService{storageDir: targetDir}, nil
		} else {
			log.Print(err)
			return nil, err
		}
	}
	return &FSVideoContentService{storageDir: targetDir}, nil
}

func (fsService *FSVideoContentService) Write(videoId string, filename string, data []byte) error {
	filePath := filepath.Join(fsService.storageDir, videoId, filename)
	_, err := os.Stat(filePath)
	if os.IsExist(err) {
		log.Printf("%s File %s already exists, ignore writing", logFSPrefix, filePath)
		return err
	}
	dirpath := filepath.Dir(filePath)
	err = os.MkdirAll(dirpath, 0755)
	if err != nil {
		log.Printf("%s %s", logFSPrefix, err)
		return err
	}
	out, err := os.Create(filePath)
	defer out.Close()
	if err != nil {
		log.Printf("%s Failed to create target file %s\n", logFSPrefix, filePath)
		return err
	}
	size, err := out.Write(data)
	if err != nil {
		log.Printf("%s Failed to write to target file %s\n", logFSPrefix, filePath)
		return err
	} else {
		log.Printf("%s Total %d bytes written to %s\n", logFSPrefix, size, filePath)
	}
	return nil
}

func (fsService *FSVideoContentService) Read(videoId string, filename string) ([]byte, error) {
	filepath := filepath.Join(fsService.storageDir, videoId, filename)
	_, err := os.Stat(filepath)
	if os.IsNotExist(err) {
		log.Printf("%s File %s not exist\n", logFSPrefix, filepath)
		return nil, err
	}
	file, err := os.Open(filepath)
	data, err := io.ReadAll(file)
	if err != nil {
		log.Printf("%s Failed to Read from file %s\n", logFSPrefix, filepath)
		return nil, err
	}
	return data, err
}

// Uncomment the following line to ensure FSVideoContentService implements VideoContentService
var _ VideoContentService = (*FSVideoContentService)(nil)
