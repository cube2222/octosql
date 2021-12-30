package telemetry

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/mitchellh/go-homedir"
	"github.com/oklog/ulid/v2"
)

var telemetryDir = func() string {
	dir, err := homedir.Dir()
	if err != nil {
		log.Fatalf("couldn't get user home directory: %s", err)
	}
	return filepath.Join(dir, ".octosql/telemetry")
}()

type event struct {
	DeviceID     string      `json:"device_id"`
	Type         string      `json:"type"`
	Version      string      `json:"version"`
	OS           string      `json:"os"`
	Architecture string      `json:"architecture"`
	NumCPU       int         `json:"num_cpu"`
	Time         time.Time   `json:"time"`
	Data         interface{} `json:"data"`
}

func SendTelemetry(ctx context.Context, eventType string, data interface{}) {
	if err := sendTelemetry(ctx, eventType, data); err != nil {
		log.Printf("couldn't send telemetry: %s", err)
	}
}

func sendTelemetry(ctx context.Context, eventType string, data interface{}) error {
	deviceIDBytes, err := os.ReadFile(filepath.Join(telemetryDir, "device_id"))
	if os.IsNotExist(err) {
		fmt.Println(`OctoSQL sends anonymous usage statistics to help us guide the development of OctoSQL.
You can view the most recently sent usage events in the ~/.octosql/telemetry/recent directory.
You can turn telemetry off by setting the environment variable OCTOSQL_NO_TELEMETRY to 1.
Please don't though, as it helps us a great deal.'`)
		os.MkdirAll(telemetryDir, 0755)
		os.WriteFile(filepath.Join(telemetryDir, "device_id"), []byte(ulid.MustNew(ulid.Now(), rand.Reader).String()), 0644)
		return nil
	}
	if os.Getenv("OCTOSQL_NO_TELEMETRY") == "1" {
		return nil
	}
	deviceID := string(deviceIDBytes)

	var version string
	info, ok := debug.ReadBuildInfo()
	if ok {
		version = info.Main.Version
	} else {
		version = "unknown"
	}
	payload := event{
		DeviceID:     deviceID,
		Type:         eventType,
		Version:      version,
		OS:           runtime.GOOS,
		Architecture: runtime.GOARCH,
		NumCPU:       runtime.NumCPU(),
		Time:         time.Now(),
		Data:         data,
	}
	body, err := json.Marshal(&payload)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Join(telemetryDir, "pending"), 0755); err != nil {
		return err
	}

	if err := os.WriteFile(filepath.Join(telemetryDir, "pending", ulid.MustNew(ulid.Now(), rand.Reader).String()+".json"), body, 0644); err != nil {
		return err
	}

	return sendBatch(ctx)
}

func sendBatch(ctx context.Context) error {
	files, err := filepath.Glob(filepath.Join(telemetryDir, "pending/*.json"))
	if err != nil {
		return err
	}

	if len(files) == 0 {
		return nil
	}

	minimumBatchCount := 10
	if _, err := os.Stat(filepath.Join(telemetryDir, "first_message_sent")); os.IsNotExist(err) {
		minimumBatchCount = 1
	}

	if len(files)%minimumBatchCount != 0 {
		return nil
	}

	var payload []json.RawMessage
	for i := range files {
		data, err := os.ReadFile(files[i])
		if err != nil {
			return err
		}
		payload = append(payload, data)
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	if _, err := http.Post("https://telemetry.octosql.dev/telemetry", "encoding/json", bytes.NewReader(data)); err != nil {
		return err
	}

	if err := os.RemoveAll(filepath.Join(telemetryDir, "recent")); err != nil {
		return err
	}

	if err := os.Rename(filepath.Join(telemetryDir, "pending"), filepath.Join(telemetryDir, "recent")); err != nil {
		return err
	}

	if minimumBatchCount == 1 {
		os.WriteFile(filepath.Join(telemetryDir, "first_message_sent"), []byte{}, 0644)
	}

	return nil
}
