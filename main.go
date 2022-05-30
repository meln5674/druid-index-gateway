package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	flag "github.com/spf13/pflag"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"
)

func ErrorResponse(w http.ResponseWriter, statusCode int, msg string) {
	w.WriteHeader(statusCode)
	bytes, err := json.Marshal(map[string]string{"error": msg})
	if err != nil {
		// Should probably log this or something?
		return
	}
	_, err = w.Write(bytes)
	// Should probably log this if it fails?
}

func MaliciousPath(path string) bool {
	// TODO: Check if path has more ..'s than it has parts, i.e. path escapes root
	return path == "." || path == ".." || strings.HasPrefix(path, "../")
}

type TLSConfig struct {
	CertFile string
	KeyFile  string
}

func ParseTLSConfig(certPath, keyPath string) (*TLSConfig, error) {
	if len(certPath) == 0 && len(keyPath) == 0 {
		return nil, nil
	}
	if len(certPath) != 0 && len(keyPath) != 0 {
		return &TLSConfig{CertFile: certPath, KeyFile: keyPath}, nil
	}
	return nil, fmt.Errorf("Must specify both TLS key and cert, or neither")
}

type Server struct {
	ListenAddr string
	TLS        *TLSConfig
}

func (s *Server) ListenAndServe(handler http.Handler) error {
	if s.TLS == nil {
		return http.ListenAndServe(s.ListenAddr, handler)
	} else {
		return http.ListenAndServeTLS(s.ListenAddr, s.TLS.CertFile, s.TLS.KeyFile, handler)
	}
}

type FileManager struct {
	RootDir string
	// TODO: Create symlink based on index task id returned from druid to uuid-based directory name, clean up underlying directory when symlink is requested to be deleted
}

func (f *FileManager) Init() error {
	return os.MkdirAll(f.RootDir, 0700)
}

func (fm *FileManager) Put(group, itemName string, itemContents io.Reader) error {
	err := os.MkdirAll(path.Join(fm.RootDir, group), 0700)
	if err != nil {
		return err
	}
	f, err := os.Create(path.Join(fm.RootDir, group, itemName))
	if err != nil {
		return err
	}
	defer f.Close()
	io.Copy(f, itemContents)
	return nil
}

func (f *FileManager) Get(group, item string) (io.Reader, error) {
	return os.Open(path.Join(f.RootDir, group, item))
}

func (f *FileManager) Delete(group string) error {
	return os.RemoveAll(path.Join(f.RootDir, group))
}

func (f *FileManager) ListGroups() (map[string]os.FileInfo, error) {
	dir, err := os.Open(f.RootDir)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	entries, err := dir.Readdir(0)
	if err != nil {
		return nil, err
	}
	groups := map[string]os.FileInfo{}
	for _, entry := range entries {
		groups[entry.Name()] = entry
	}
	return groups, nil
}

type FileTender struct {
	Files                *FileManager
	RetentionPeriod      time.Duration
	RetentionCheckPeriod time.Duration
}

func (f *FileTender) RunRetentionCheck(now time.Time) []error {
	groups, err := f.Files.ListGroups()
	if err != nil {
		return []error{err}
	}
	errs := []error{}
	for group, info := range groups {
		if now.Sub(info.ModTime()) > f.RetentionPeriod {
			err = f.Files.Delete(group)
			if err != nil {
				errs = append(errs, err)
			}
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return errs
}

func (f *FileTender) Run(stop chan struct{}) {
	ticker := time.NewTicker(f.RetentionCheckPeriod)
	for {
		select {
		case tick := <-ticker.C:
			f.RunRetentionCheck(tick)
		case _ = <-stop:
			return
		}
	}
}

const SubmitterEndpoint = "/task"

type Submitter struct {
	Server
	ContextPath          string
	Files                *FileManager
	DruidIndexerEndpoint url.URL
	FetchURLBase         url.URL
}

func (s *Submitter) Handle(mux *http.ServeMux) {
	mux.HandleFunc(s.ContextPath+SubmitterEndpoint, s.Task)
	mux.HandleFunc(s.ContextPath+SubmitterEndpoint+"/", s.Task)
	mux.HandleFunc(s.ContextPath+"/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
}

func (s *Submitter) Task(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "DELETE":
		s.Cleanup(w, r)
		return
	case "POST":
		s.Index(w, r)
		return
	default:
		ErrorResponse(w, http.StatusMethodNotAllowed, BadIndexTaskMethodMsg)
		return
	}
}

const BadIndexTaskMethodMsg = "/task endpoint supports POST for submitting tasks, and /task/{group} supports DELETE for cleaning up file sets"

const BadIndexTaskMsg = "Task submissions must be a multi-part upload with the task spec as the first part, and all files to ingest as the remaining parts with filenames"

const BadIndexTaskSpecMsg = "Task spec must be an index or index_parallel type task, complete except for .spec.ioConfig.inputSource, in valid JSON encoding"

const InternalErrorMsg = "Internal Error"

func (s *Submitter) Index(w http.ResponseWriter, r *http.Request) {
	fmt.Println(*r)
	multipart, err := r.MultipartReader()
	if err != nil {
		ErrorResponse(w, http.StatusBadRequest, BadIndexTaskMsg)
		return
	}
	group := uuid.New().String()
	part, err := multipart.NextPart()
	if err != nil {
		ErrorResponse(w, http.StatusBadRequest, BadIndexTaskMsg)
		return
	}
	taskSpec := map[string]interface{}{}
	err = json.NewDecoder(part).Decode(&taskSpec)
	if err != nil {
		fmt.Println(err)
		ErrorResponse(w, http.StatusBadRequest, BadIndexTaskSpecMsg)
		return
	}
	fmt.Printf("%#v\n", taskSpec)

	spec, ok := taskSpec["spec"].(map[string]interface{})
	if !ok || (taskSpec["type"] != "index" && taskSpec["type"] != "index_parallel") {
		ErrorResponse(w, http.StatusBadRequest, BadIndexTaskSpecMsg)
		return
	}
	ioConfig, ok := spec["ioConfig"].(map[string]interface{})
	if !ok {
		ErrorResponse(w, http.StatusBadRequest, BadIndexTaskSpecMsg)
		return
	}

	uris := []string{}
	var successful bool
	defer func() {
		if !successful {
			s.Files.Delete(group)
		}
	}()
	for part, err = multipart.NextPart(); err == nil; part, err = multipart.NextPart() {
		filename := strings.TrimPrefix(strings.TrimPrefix(part.FileName(), "/"), "./")
		fmt.Println(filename)
		if len(filename) == 0 || MaliciousPath(filename) {
			ErrorResponse(w, http.StatusBadRequest, BadIndexTaskMsg)
			return
		}
		err = s.Files.Put(group, filename, part)
		if err != nil {
			fmt.Println(err)
			ErrorResponse(w, http.StatusInternalServerError, InternalErrorMsg)
			return
		}
		fetchURL := s.FetchURLBase
		fetchURL.Path += group + "/" + filename
		uris = append(uris, fetchURL.String())
	}
	if err != nil && err != io.EOF {
		ErrorResponse(w, http.StatusBadRequest, BadIndexTaskMsg)
		return
	}

	inputSource := map[string]interface{}{}
	inputSource["type"] = "http"
	inputSource["uris"] = uris
	ioConfig["inputSource"] = inputSource
	// TODO: Option for authentication if TLS is enabled both ways?

	taskSpecBytes, err := json.Marshal(taskSpec)
	if err != nil {
		fmt.Println(err)
		ErrorResponse(w, http.StatusInternalServerError, InternalErrorMsg)
		return
	}
	fmt.Println(string(taskSpecBytes))
	taskResponse, err := http.Post(s.DruidIndexerEndpoint.String(), "application/json", bytes.NewReader(taskSpecBytes))
	if err != nil {
		fmt.Println(err)
		ErrorResponse(w, http.StatusInternalServerError, InternalErrorMsg)
		return
	}
	defer taskResponse.Body.Close()
	if taskResponse.StatusCode == http.StatusOK {
		successful = true
	}
	for name, values := range taskResponse.Header {
		w.Header()[name] = values
	}
	w.WriteHeader(taskResponse.StatusCode)
	io.Copy(w, taskResponse.Body)
	// Should probably log this if it fails
}

const BadFileMsg = "Unknown or Illegal Group or File"

func (s *Submitter) Cleanup(w http.ResponseWriter, r *http.Request) {
	group := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, s.ContextPath+SubmitterEndpoint), "/")
	// No subdirs or relative paths allowed, only single basenames
	if strings.Contains(group, "/") || MaliciousPath(group) {
		ErrorResponse(w, http.StatusNotFound, BadFileMsg)
		return
	}
	err := s.Files.Delete(group)
	if err != nil {
		ErrorResponse(w, http.StatusNotFound, BadFileMsg)
		return
	}
}

const RetrieverEndpoint = "/file"

const BadFetchMethodMsg = "/file endpoints only support GET"

type Retriever struct {
	Server
	ContextPath string
	Files       *FileManager
}

func (r *Retriever) Handle(mux *http.ServeMux) {
	mux.HandleFunc(r.ContextPath+RetrieverEndpoint+"/", r.Fetch)
	mux.HandleFunc(r.ContextPath+"/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
}

func (rt *Retriever) Fetch(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		ErrorResponse(w, http.StatusMethodNotAllowed, BadFetchMethodMsg)
		return
	}
	requestedItem := strings.TrimPrefix(r.URL.Path, rt.ContextPath+RetrieverEndpoint+"/")
	parts := strings.SplitN(requestedItem, "/", 2)
	group := parts[0]
	item := parts[1]
	if len(group) == 0 || MaliciousPath(group) || len(item) == 0 || MaliciousPath(item) {
		ErrorResponse(w, http.StatusNotFound, BadFileMsg)
		return
	}
	itemContents, err := rt.Files.Get(group, item)
	if err != nil {
		if err == os.ErrNotExist {
			ErrorResponse(w, http.StatusNotFound, BadFileMsg)
			return
		} else {
			ErrorResponse(w, http.StatusInternalServerError, InternalErrorMsg)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
	io.Copy(w, itemContents)
	// Should probably log this if it fails
}

type Combined struct {
	Server
	SubmitterContextPath string
	RetrieverContextPath string
	Files                *FileManager
	DruidIndexerEndpoint url.URL // Should end with druid/indexer/v1/task
	FetchURLBase         url.URL
}

func (c *Combined) Handle(mux *http.ServeMux) {
	(&Submitter{
		Server:               c.Server,
		ContextPath:          c.SubmitterContextPath,
		Files:                c.Files,
		DruidIndexerEndpoint: c.DruidIndexerEndpoint,
		FetchURLBase:         c.FetchURLBase,
	}).Handle(mux)
	(&Retriever{
		Server:      c.Server,
		ContextPath: c.RetrieverContextPath,
		Files:       c.Files,
	}).Handle(mux)
}

var (
	tasksAddr            = flag.String("tasks-addr", ":8080", "Listen address for task submissions and cleanup")
	tasksContextPath     = flag.String("tasks-context-path", "/tasks", "URL Sub-path for task submissions and cleanup")
	tasksTLSCertPath     = flag.String("tasks-tls-cert", "", "Path to TLS certificate for task submissions and cleanup")
	tasksTLSKeyPath      = flag.String("tasks-tls-key", "", "Path to TLS key for task submissions and cleanup")
	druidIndexerEndpoint = flag.String("druid-indexer-endpoint", "http://localhost:8888/druid/indexer/v1/task", "URL to sent Druid tasks to")

	filesAddr        = flag.String("files-addr", ":8080", "Listen address for retrieving submitted files")
	filesContextPath = flag.String("files-context-path", "/files", "URL Sub-path for retrieving submitted files")
	filesTLSCertPath = flag.String("files-tls-cert", "", "Path to TLS certificate for retrieving submitted files")
	filesTLSKeyPath  = flag.String("files-tls-key", "", "Path to TLS key for retrieving submitted files")
	filesExternalURL = flag.String("files-external-url", "", "Root URL files will be accessible to Druid from. Defaults to http(s)://{files-addr}{files-context-path}/files/, depending on whether or not TLS certs are provided")

	sharedTLSCertPath = flag.String("tls-cert", "", "Path to TLS certificate when listening on the same address for both tasks and files")
	sharedTLSKeyPath  = flag.String("tls-key", "", "Path to TLS key when listening on the same address for both tasks and files")

	retentionPeriod      = flag.Duration("retention-period", time.Hour*1, "How long to retain submitted files before automatic deletion")
	retentionCheckPeriod = flag.Duration("retention-check-period", time.Hour*1, "How frequently to check for submitted files which have passed the retention period")

	rootDir = flag.String("root-dir", "/tmp/druid-index-gateway", "Root directory to store submitted files")
)

func main() {
	flag.Parse()

	stopChan := make(chan struct{})

	fileManager := FileManager{RootDir: *rootDir}
	filesExternalURLStr := *filesExternalURL
	var needProtocolPrefix bool
	if len(filesExternalURLStr) == 0 {
		filesExternalURLStr = *filesAddr + *filesContextPath + RetrieverEndpoint + "/"
		needProtocolPrefix = true
	}
	druidIndexerURL, err := url.Parse(*druidIndexerEndpoint)
	if err != nil {
		fmt.Println(err)
		return
	}
	if *tasksAddr == *filesAddr {
		if strings.HasPrefix(*filesContextPath, *tasksContextPath) || strings.HasPrefix(*tasksContextPath, *filesContextPath) {
			fmt.Println("--files-context-path and --tasks-context-path must not overlap when running on the same interface and port")
			return
		}
		tlsConfig, err := ParseTLSConfig(*sharedTLSCertPath, *sharedTLSKeyPath)
		if err != nil {
			fmt.Println(err)
			return
		}
		if tlsConfig == nil && needProtocolPrefix {
			filesExternalURLStr = "http://" + filesExternalURLStr
		}
		if tlsConfig != nil && needProtocolPrefix {
			filesExternalURLStr = "https://" + filesExternalURLStr
		}
		filesExternalURLParsed, err := url.Parse(filesExternalURLStr)
		if err != nil {
			fmt.Println(err)
			return
		}
		combined := Combined{
			Server: Server{
				ListenAddr: *tasksAddr,
				TLS:        tlsConfig,
			},
			SubmitterContextPath: *tasksContextPath,
			RetrieverContextPath: *filesContextPath,
			Files:                &fileManager,
			DruidIndexerEndpoint: *druidIndexerURL,
			FetchURLBase:         *filesExternalURLParsed,
		}
		mux := http.NewServeMux()
		combined.Handle(mux)
		fmt.Printf("Listening on %s\n", *tasksAddr)
		go func() {
			fmt.Println(combined.ListenAndServe(mux))
			close(stopChan)
		}()
	} else {
		filesTLSConfig, err := ParseTLSConfig(*filesTLSCertPath, *filesTLSKeyPath)
		if err != nil {
			fmt.Println(err)
			return
		}
		tasksTLSConfig, err := ParseTLSConfig(*tasksTLSCertPath, *tasksTLSKeyPath)
		if err != nil {
			fmt.Println(err)
			return
		}
		if filesTLSConfig == nil && needProtocolPrefix {
			filesExternalURLStr = "http://" + filesExternalURLStr
		}
		if filesTLSConfig != nil && needProtocolPrefix {
			filesExternalURLStr = "https://" + filesExternalURLStr
		}
		filesExternalURLParsed, err := url.Parse(filesExternalURLStr)
		if err != nil {
			fmt.Println(err)
			return
		}
		retrieverMux := http.NewServeMux()
		retriever := Retriever{
			Server: Server{
				ListenAddr: *filesAddr,
				TLS:        filesTLSConfig,
			},
			ContextPath: *filesContextPath,
			Files:       &fileManager,
		}
		retriever.Handle(retrieverMux)
		submitterMux := http.NewServeMux()
		submitter := Submitter{
			Server: Server{
				ListenAddr: *tasksAddr,
				TLS:        tasksTLSConfig,
			},
			ContextPath:          *tasksContextPath,
			Files:                &fileManager,
			DruidIndexerEndpoint: *druidIndexerURL,
			FetchURLBase:         *filesExternalURLParsed,
		}
		submitter.Handle(submitterMux)
		fmt.Printf("Listening on %s\n", *filesAddr)
		go func() {
			fmt.Println(retriever.ListenAndServe(retrieverMux))
			close(stopChan)
		}()
		fmt.Printf("Listening on %s\n", *tasksAddr)
		go func() {
			fmt.Println(submitter.ListenAndServe(submitterMux))
			close(stopChan)
		}()
	}

	(&FileTender{
		Files:                &fileManager,
		RetentionPeriod:      *retentionPeriod,
		RetentionCheckPeriod: *retentionCheckPeriod,
	}).Run(stopChan)
}
