package rustlib

import (
	_ "embed"
)

//go:embed target/wasm32-wasi/release/rustlib.wasm
var Module []byte
