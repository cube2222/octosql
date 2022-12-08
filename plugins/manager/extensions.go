package manager

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/cube2222/octosql/config"
)

var octosqlFileExtensionHandlersFile = func() string {
	return filepath.Join(config.OctosqlConfigDir, "file_extension_handlers.json")
}()

func (*PluginManager) GetFileExtensionHandlers() (map[string]string, error) {
	return loadFileExtensionHandlers()
}

func registerFileExtensions(name string, extensions []string) error {
	handlers, err := loadFileExtensionHandlers()
	if err != nil {
		return err
	}
	for _, ext := range extensions {
		if oldName, ok := handlers[ext]; ok && oldName != name {
			log.Printf("file extension handler for %s already registered, overwriting", ext)
		}
		handlers[ext] = name
	}
	return saveFileExtensionHandlers(handlers)
}

func loadFileExtensionHandlers() (map[string]string, error) {
	data, err := os.ReadFile(octosqlFileExtensionHandlersFile)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]string{}, nil
		}
		return nil, fmt.Errorf("couldn't read file extension handlers file: %w", err)
	}
	var handlers map[string]string
	if err := json.Unmarshal(data, &handlers); err != nil {
		return nil, fmt.Errorf("couldn't json-decode file extension handlers file: %w", err)
	}
	return handlers, nil
}

func saveFileExtensionHandlers(handlers map[string]string) error {
	data, err := json.Marshal(handlers)
	if err != nil {
		return fmt.Errorf("couldn't json-encode file extension handlers: %w", err)
	}
	if err := os.WriteFile(octosqlFileExtensionHandlersFile, data, 0644); err != nil {
		return fmt.Errorf("couldn't write file extension handlers to file: %w", err)
	}
	return nil
}
