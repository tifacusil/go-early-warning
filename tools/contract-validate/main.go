// Command contract-validate checks that every contracts/examples/<name>.example.json
// validates against contracts/schemas/<name>.schema.json (JSON Schema Draft 2020-12),
// and that each schema declares a semantic schema_version, per CLAUDE.md §1.4 / §3.
//
// Exit 0 if all examples validate; exit 1 (with details) otherwise.
// Run via `make contract-validate`.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

const (
	schemasDir  = "contracts/schemas"
	examplesDir = "contracts/examples"
)

var semverRe = regexp.MustCompile(`^[0-9]+\.[0-9]+$`)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "contract-validate: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	schemaFiles, err := filepath.Glob(filepath.Join(schemasDir, "*.schema.json"))
	if err != nil {
		return err
	}
	if len(schemaFiles) == 0 {
		fmt.Println("contract-validate: no schemas found — nothing to validate")
		return nil
	}
	sort.Strings(schemaFiles)

	c := jsonschema.NewCompiler()
	var failures []string

	for _, sf := range schemaFiles {
		name := strings.TrimSuffix(filepath.Base(sf), ".schema.json")

		schema, err := c.Compile(sf)
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s: compile schema: %v", name, err))
			continue
		}

		exFile := filepath.Join(examplesDir, name+".example.json")
		if _, err := os.Stat(exFile); err != nil {
			failures = append(failures, fmt.Sprintf("%s: missing example %s", name, exFile))
			continue
		}

		inst, err := loadJSON(exFile)
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s: read example: %v", name, err))
			continue
		}

		if err := schema.Validate(inst); err != nil {
			failures = append(failures, fmt.Sprintf("%s: example invalid:\n%v", name, err))
			continue
		}

		if err := checkSchemaVersion(inst); err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", name, err))
			continue
		}

		fmt.Printf("OK  %-16s <- %s\n", name, filepath.Base(exFile))
	}

	if len(failures) > 0 {
		return fmt.Errorf("%d contract(s) failed:\n- %s", len(failures), strings.Join(failures, "\n- "))
	}
	fmt.Printf("contract-validate: %d schema(s) OK\n", len(schemaFiles))
	return nil
}

// checkSchemaVersion enforces the semantic major.minor envelope (CLAUDE.md §1.4).
func checkSchemaVersion(inst any) error {
	m, ok := inst.(map[string]any)
	if !ok {
		return fmt.Errorf("root is not an object")
	}
	v, ok := m["schema_version"].(string)
	if !ok {
		return fmt.Errorf("missing string schema_version")
	}
	if !semverRe.MatchString(v) {
		return fmt.Errorf("schema_version %q not major.minor", v)
	}
	return nil
}

func loadJSON(path string) (any, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return jsonschema.UnmarshalJSON(f)
}
