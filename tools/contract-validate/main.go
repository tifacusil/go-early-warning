// Command contract-validate checks that every contracts/examples/*.json validates
// against its JSON Schema in contracts/schemas/, per CLAUDE.md §3 (contract tests).
// Run via `make contract-validate`.
package main

import (
	"fmt"
	"os"
)

func main() {
	// TODO(SE-practice): load contracts/schemas + contracts/examples, validate each
	// example against its schema (Draft 2020-12), and enforce the semantic
	// versioning rule on schema_version (Hito 1). Skeleton: succeeds so CI is green
	// until Hito 1 lands the contracts.
	fmt.Fprintln(os.Stderr, "contract-validate: no contracts yet (Hito 1 pending) — OK")
}
