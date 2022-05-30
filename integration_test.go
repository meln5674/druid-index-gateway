package main

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

const (
	SampleDataURL              = "https://github.com/apache/druid/raw/master/examples/quickstart/tutorial/wikiticker-2015-09-12-sampled.json.gz"
	SampleDataDownloadLocation = "tmp/wikiticker-2015-09-12-sampled.json.gz"
	SampleIndexSpecURL         = "https://raw.githubusercontent.com/apache/druid/master/examples/quickstart/tutorial/wikipedia-index.json"
	SampleQuery                = `
SELECT
  "page",
  "countryName",
  COUNT(*) AS "Edits"
FROM "wikipedia"
GROUP BY 1, 2
ORDER BY "Edits" DESC
`

	DruidReleaseURL              = "https://dlcdn.apache.org/druid/0.22.1/apache-druid-0.22.1-bin.tar.gz"
	DruidReleaseDownloadLocation = "tmp/druid-bin.tar.gz"
	DruidReleaseExtractLocation  = "tmp/druid-bin"
	DruidStartScript             = "apache-druid-0.22.1/bin/start-nano-quickstart"
)

func ensureDownloadedFile(dest, src string) error {
	_, err := os.Stat(dest)
	if err == nil {
		return nil
	}
	if !os.IsNotExist(os.ErrNotExist) {
		return err
	}
	resp, err := http.Get(src)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	err = os.MkdirAll(filepath.Dir(dest), 0700)
	if err != nil {
		return err
	}
	f, err := os.Create(dest)
	if err != nil {
		return err
	}
	io.Copy(f, resp.Body)
	return nil
}

func extractTarGZ(dest, src string) error {
	err := os.MkdirAll(dest, 0700)
	if err != nil {
		return err
	}
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	tarball := tar.NewReader(gz)
	for header, err := tarball.Next(); err == nil; header, err = tarball.Next() {
		err = func() error {
			var err error
			fileDest := path.Join(dest, header.Name)
			switch header.Typeflag {
			case tar.TypeReg:
				err = os.MkdirAll(filepath.Dir(fileDest), 0700)
				if err != nil {
					return err
				}
				f, err := os.Create(fileDest)
				if err != nil {
					return err
				}
				defer f.Close()
				io.CopyN(f, tarball, header.Size)
				err = os.Chmod(fileDest, os.FileMode(header.Mode))
				if err != nil {
					return err
				}
			}
			// Probably won't need to deal with links or special files
			return nil
		}()
		if err != nil {
			return err
		}
	}
	return err
}

func waitForServiceHealthy(t *testing.T, url string) {
	for {
		resp, err := http.Get(url)
		if err != nil {
			t.Log("Not yet Healthy", err)
			time.Sleep(5 * time.Second)
			continue
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Log("Not yet Healthy", resp.Status)
			time.Sleep(5 * time.Second)
			continue
		}
		break
	}
}

func druidStartupIsDumb(t *testing.T, url string) {
	for {
		resp, err := http.Post(url, "application/json", strings.NewReader("{}"))
		if err != nil {
			t.Log("Not yet Healthy", err)
			time.Sleep(5 * time.Second)
			continue
		}
		resp.Body.Close()
		break
	}
}

func submitTask(url string, specJSON []byte, dataFile string) (string, error) {
	f, err := os.Open(dataFile)
	if err != nil {
		return "", err
	}
	/*info, err := f.Stat()
	if err != nil {
		return "", err
	}
	dataFileSize := info.Size()
	*/

	r, w := io.Pipe()
	body := multipart.NewWriter(w)

	var uploadErr error
	go func(errPtr *error) {
		defer w.Close()
		defer f.Close()
		specPart, err := body.CreateFormField("spec.json")
		if err != nil {
			*errPtr = err
			fmt.Println(err)
			return
		}

		_, err = specPart.Write(specJSON)
		if err != nil {
			*errPtr = err
			fmt.Println(err)
			return
		}

		dataPart, err := body.CreateFormFile("file", dataFile)
		if err != nil {
			*errPtr = err
			fmt.Println(err)
			return
		}
		_, err = io.Copy(dataPart, f)
		if err != nil {
			*errPtr = err
			fmt.Println(err)
			return
		}

		*errPtr = body.Close()
	}(&uploadErr)

	resp, err := http.Post(url, body.FormDataContentType(), r)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return "", fmt.Errorf(string(body))
	}

	respJson := map[string]interface{}{}
	err = json.NewDecoder(resp.Body).Decode(&respJson)
	if err != nil {
		return "", err
	}
	taskID, ok := respJson["task"].(string)
	if !ok {
		return "", fmt.Errorf("Got something other than a string for Druid task ID")
	}
	return taskID, nil
}

func waitForTask(t *testing.T, url string) error {
	for {
		resp, err := http.Get(url)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			body, _ := ioutil.ReadAll(resp.Body)
			return fmt.Errorf(string(body))
		}
		respJson := map[string]interface{}{}
		err = json.NewDecoder(resp.Body).Decode(&respJson)
		if err != nil {
			return err
		}
		status, ok := respJson["status"].(map[string]interface{})
		if !ok {
			return fmt.Errorf("Unexpected JSON structure from Druid")
		}
		taskState, ok := status["statusCode"].(string)
		if !ok {
			return fmt.Errorf("Unexpected JSON structure from Druid")
		}
		switch taskState {
		case "RUNNING":
			t.Log("Task still running")
			time.Sleep(5 * time.Second)
			continue
		case "SUCCESS":
			return nil
		case "FAILED":
			return fmt.Errorf("Task Failed: %#v", respJson)
		default:
			return fmt.Errorf("Got unexpected taskState from Druid: %s", taskState)
		}
	}
}

func query(url, sql string) (string, error) {
	sqlBytes, err := json.Marshal(map[string]string{"query": sql})
	if err != nil {
		return "", err
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(sqlBytes))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	respStr := string(respBytes)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf(respStr)
	}
	return respStr, nil
}

func captureLogs(t *testing.T, cmd *exec.Cmd, prefix string) error {
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			t.Log(prefix, scanner.Text())
		}
	}()
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			t.Log(prefix, scanner.Text())
		}
	}()
	return nil
}

func Test(t *testing.T) {
	t.Log("Downloading sample data...")
	err := ensureDownloadedFile(SampleDataDownloadLocation, SampleDataURL)
	if err != nil {
		t.Log("Failed to download sample data", err)
		t.Fail()
		return
	}
	t.Log("Downloading Druid Release...")
	err = ensureDownloadedFile(DruidReleaseDownloadLocation, DruidReleaseURL)
	if err != nil {
		t.Log("Failed to download Druid release", err)
		t.Fail()
		return
	}
	t.Log("Extracting Druid Release...")
	err = extractTarGZ(DruidReleaseExtractLocation, DruidReleaseDownloadLocation)
	if err != nil {
		t.Log("Failed to extract Druid release", err)
		t.Fail()
		return
	}

	t.Log("Downloading sample index spec JSON...")
	resp, err := http.Get(SampleIndexSpecURL)
	if err != nil {
		t.Log("Failed to download sample index spec JSON")
		t.Fail()
		return
	}
	defer resp.Body.Close()
	indexSpec, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Log("Failed to download sample index spec JSON")
		t.Fail()
		return
	}

	t.Log("Starting Druid...")

	druid := exec.Command(
		path.Join(DruidReleaseExtractLocation, DruidStartScript),
	)
	err = captureLogs(t, druid, "druid says:")
	if err != nil {
		t.Log("Failed to start Druid", err)
		t.Fail()
		return
	}
	err = druid.Start()
	if err != nil {
		t.Log("Failed to start Druid", err)
		t.Fail()
		return
	}
	defer druid.Process.Signal(syscall.SIGTERM)
	waitForServiceHealthy(t, "http://127.0.0.1:8888/status/health")
	waitForServiceHealthy(t, "http://127.0.0.1:8081/status/health")
	druidStartupIsDumb(t, "http://127.0.0.1:8888/druid/indexer/v1/task")

	t.Log("Starting Druid Index Gateway...")
	indexGateway := exec.Command("go", "run", "main.go", "--tasks-addr", ":8180", "--files-addr", ":8180", "--root-dir", "tmp/files")
	err = captureLogs(t, indexGateway, "index gateway says:")
	if err != nil {
		t.Log("Failed to start Druid Index Gateway", err)
		t.Fail()
		return
	}
	err = indexGateway.Start()
	if err != nil {
		t.Log("Failed to start Druid Index Gateway", err)
		t.Fail()
		return
	}
	defer indexGateway.Process.Signal(syscall.SIGTERM)
	waitForServiceHealthy(t, "http://127.0.0.1:8180/tasks/health")
	waitForServiceHealthy(t, "http://127.0.0.1:8180/files/health")

	t.Log("Submitting Task...")
	taskID, err := submitTask("http://127.0.0.1:8180/tasks/task", indexSpec, SampleDataDownloadLocation)
	if err != nil {
		t.Log("Failed to submit task", err)
		t.Fail()
		time.Sleep(5 * time.Second)
		return
	}

	t.Log("Waiting for Task to Complete...")
	err = waitForTask(t, fmt.Sprintf("http://127.0.0.1:8888/druid/indexer/v1/task/%s/status", taskID))
	if err != nil {
		t.Log("Failed to wait for Task completion", err)
		t.Fail()
		return
	}
	t.Log("Querying Druid...")

	results, err := query("http://127.0.0.1:8888/druid/v2/sql", SampleQuery)
	if err != nil {
		t.Log("Failed to query druid", err)
		t.Fail()
		return
	}
	t.Log(results)
}
