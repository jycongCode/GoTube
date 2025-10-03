// Lab 7: Implement a web server

package web

import (
	"fmt"
	"html/template"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type server struct {
	Addr string
	Port int

	metadataService VideoMetadataService
	contentService  VideoContentService

	mux *http.ServeMux
}

func NewServer(
	metadataService VideoMetadataService,
	contentService VideoContentService,
) *server {
	return &server{
		metadataService: metadataService,
		contentService:  contentService,
	}
}

func (s *server) Start(lis net.Listener) error {
	s.mux = http.NewServeMux()
	s.mux.HandleFunc("/upload", s.handleUpload)
	s.mux.HandleFunc("/videos/", s.handleVideo)
	s.mux.HandleFunc("/content/", s.handleVideoContent)
	s.mux.HandleFunc("/", s.handleIndex)

	return http.Serve(lis, s.mux)
}

func (s *server) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	// panic("Lab 7: not implemented")
	var videoRecords []map[string]string
	videoMetaDataList, err := s.metadataService.List()
	if err != nil {
		http.Error(w, "Video list retrieve failure", http.StatusBadRequest)
		return
	}
	for _, metadata := range videoMetaDataList {
		videoRecords = append(videoRecords,
			map[string]string{
				"Id":         metadata.Id,
				"UploadTime": metadata.UploadedAt.Format("2006-01-02 15:04:05"),
				"EscapedId":  url.PathEscape(metadata.Id),
			},
		)
	}
	var indexTemplate = template.New("index.html")
	indexTemplate.Parse(indexHTML)
	err = indexTemplate.Execute(w, videoRecords)
	if err != nil {
		http.Error(w, "Error rendering index.html template", 500)
	}
}

func (s *server) handleUpload(w http.ResponseWriter, r *http.Request) {
	// panic("Lab 7: not implemented")
	err := r.ParseMultipartForm(10 << 20)
	if err != nil {
		log.Printf("ERROR: %s\n", err)
		http.Error(w, "Error parsing multipart/form-data format", http.StatusBadRequest)
		return
	}
	file, fileHeader, err := r.FormFile("file")
	if err != nil {
		log.Printf("ERROR: %s\n", err)
		http.Error(w, "No field of file", http.StatusBadRequest)
		return
	}
	defer file.Close()
	filename := fileHeader.Filename

	videoId := strings.Split(filename, ".")[0]

	_, err = s.metadataService.Read(videoId)

	if err == nil {
		http.Error(w, "Video Id conflict", http.StatusConflict)
		return
	}

	tmpDir, err := os.MkdirTemp("./", "tmp*")
	if err != nil {
		log.Printf("Failed to create temp dir %s\n", tmpDir)
		return
	}
	tmpFilePath := filepath.Join(tmpDir, fileHeader.Filename)
	tmpFile, err := os.Create(tmpFilePath)
	if err != nil {
		log.Printf("Failed to create tmp file %s", tmpFilePath)
		return
	}
	_, err = io.Copy(tmpFile, file)
	tmpFile.Close()
	if err != nil {
		log.Printf("Failed to store tmp file %s on disk\n", tmpFilePath)
		return
	}

	manifestPath := filepath.Join(tmpDir, "manifest.mpd")

	cmd := exec.Command("ffmpeg",
		"-i", tmpFilePath, // input file
		"-c:v", "libx264", // video codec
		"-c:a", "aac", // audio codec
		"-bf", "1", // max 1 b-frame
		"-keyint_min", "120", // minimum keyframe interval
		"-g", "120", // keyframe every 120 frames
		"-sc_threshold", "0", // scene change threshold
		"-b:v", "3000k", // video bitrate
		"-b:a", "128k", // audio bitrate
		"-f", "dash", // dash format
		"-use_timeline", "1", // use timeline
		"-use_template", "1", // use template
		"-init_seg_name", "init-$RepresentationID$.m4s", // init segment naming
		"-media_seg_name", "chunk-$RepresentationID$-$Number%05d$.m4s", // media segment naming
		"-seg_duration", "4", // segment duration in seconds
		manifestPath)
	out, err := cmd.Output()
	fmt.Println(string(out))

	entries, err := os.ReadDir(tmpDir)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		ext := strings.ToLower(filepath.Ext(entry.Name()))
		if ext != "mp4" {
			mdfilePath := filepath.Join(tmpDir, entry.Name())
			data, err := os.ReadFile(mdfilePath)
			if err != nil {
				log.Printf("Failed to read file %s\n", mdfilePath)
				continue
			}
			s.contentService.Write(videoId, entry.Name(), data)
		}
	}
	os.RemoveAll(tmpDir)
	uploadTime := time.Now()

	err = s.metadataService.Create(videoId, uploadTime)
	if err != nil {
		log.Printf("ERROR: %s", err)
		http.Error(w, "Video Id conflict", http.StatusConflict)
		return
	}
	http.Redirect(w, r, "/", http.StatusSeeOther)

}

func (s *server) handleVideo(w http.ResponseWriter, r *http.Request) {
	videoId := r.URL.Path[len("/videos/"):]
	var videoTemplate = template.New("video.html")
	videoTemplate.Parse(videoHTML)
	metaData, err := s.metadataService.Read(videoId)
	if err != nil {
		http.Error(w, "No such video", http.StatusNotFound)
		return
	}
	templateData := map[string]string{
		"Id":         metaData.Id,
		"UploadedAt": metaData.UploadedAt.Format("2006-01-02 15:04:05"),
	}
	err = videoTemplate.Execute(w, templateData)
	if err != nil {
		http.Error(w, "Error rendering index.html template", 500)
	}
	// panic("Lab 7: not implemented")
}

func (s *server) handleVideoContent(w http.ResponseWriter, r *http.Request) {
	// parse /content/<videoId>/<filename>
	videoId := r.URL.Path[len("/content/"):]
	parts := strings.Split(videoId, "/")
	if len(parts) != 2 {
		http.Error(w, "Invalid content path", http.StatusBadRequest)
		return
	}
	videoId = parts[0]
	filename := parts[1]
	log.Println("Video ID:", videoId, "Filename:", filename)
	extension := filepath.Ext(filename)
	switch strings.ToLower(extension) {
	case ".mpd":
		w.Header().Set("Content-Type", "application/dash+xml")
	case ".m4s":
		w.Header().Set("Content-Type", "video/iso.segment")
	case ".webm":
		w.Header().Set("Content-Type", "video/webm")
	default:
		http.Error(w, "Content not found", 500)
		return
	}
	data, err := s.contentService.Read(videoId, filename)
	if err != nil {
		http.Error(w, "File not found", 500)
		return
	}
	w.Write(data)
}
