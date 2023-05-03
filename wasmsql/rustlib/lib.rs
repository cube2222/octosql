// Build using `env RUSTFLAGS="-Clink-arg=--no-gc-sections -Clink-arg=--relocatable" cargo build --release --target wasm32-wasi`
// It will include a malloc and free implementation and put them in the wasmsql module, which is nice.

// TODO: Use these not for all "library functions" but just to implement rustlib.

extern crate core;

use std::alloc::Layout;
use std::io::BufRead;
use std::mem;
use std::path::Path;

// use serde_json;

extern "C" {
    fn hostfunc0_0(fn_index: u32);
    fn hostfunc1_0(fn_index: u32, arg0: u32);
    fn hostfunc2_0(fn_index: u32, arg0: u32, arg1: u32);
    fn hostfunc0_1(fn_index: u32) -> u32;
    fn hostfunc1_1(fn_index: u32, arg0: u32) -> u32;
    fn hostfunc2_1(fn_index: u32, arg0: u32, arg1: u32) -> u32;
    fn debug(name: u32, x: u32) -> u32;
}

#[no_mangle]
pub unsafe extern "C" fn force_import_keep() {
    hostfunc0_0(0);
    hostfunc1_0(0, 0);
    hostfunc2_0(0, 0, 0);
    hostfunc0_1(0);
    hostfunc1_1(0, 0);
    hostfunc2_1(0, 0, 0);
    debug(0, 0);
}

#[no_mangle]
pub unsafe extern "C" fn add(x: i32, y: i32) -> i32 {
    x + y
}

#[no_mangle]
pub extern "C" fn hello() -> u32 {
    let vec: Vec<_> = "hellohelloo".into();
    Box::into_raw(vec.into_boxed_slice()) as *mut u8 as u32
}

pub unsafe fn read_wasm_string(string_header_ptr: u32) -> String {
    let ptr = *(string_header_ptr as *const u32);
    let length = *((string_header_ptr + 4) as *const u32);
    String::from_raw_parts(ptr as *mut u8, length as usize, length as usize)
}

pub unsafe fn save_wasm_string(string: String) -> u32 {
    let len = string.len() as u32;
    let out: Vec<_> = string.into();
    let new_body_ptr = Box::into_raw(out.into_boxed_slice()) as *mut u8 as u32;

    let new_header_ptr = Box::into_raw(Box::new([new_body_ptr, len])) as *mut u8 as u32;

    new_header_ptr
}

#[no_mangle]
pub unsafe extern "C" fn upper(string_header_ptr: u32) -> u32 {
    save_wasm_string(read_wasm_string(string_header_ptr).to_uppercase())
}

#[no_mangle]
pub unsafe extern "C" fn sql_malloc(size: u32) -> *mut u8 {
    std::alloc::alloc(Layout::from_size_align_unchecked(size as usize, 4))
}

#[no_mangle]
pub unsafe extern "C" fn sql_free(ptr: *mut u8) {
    std::alloc::dealloc(ptr, Layout::from_size_align_unchecked(1 as usize /* this is a hack :) it seems like dlmalloc doesn't care about the size */, 1))
}

#[no_mangle]
pub unsafe extern "C" fn open_file(filepath: u32) -> u32 {
    // let filepath = read_wasm_string(filepath);
    // TODO: This... isn't very fast.
    let file = match std::fs::File::open(Path::new("/Users/jakub/Projects/octosql/amazon/books_10k.json")) {
        Ok(file) => file,
        Err(msg) => {
            let wasm_str = save_wasm_string(msg.to_string());
            debug(42, wasm_str);
            return 1;
        }
    };
    let reader = std::io::BufReader::with_capacity(1024*1024*8, file);
    Box::into_raw(Box::new(reader)) as u32
}

#[no_mangle]
pub unsafe extern "C" fn read_json_record(reader_ptr: u32, record_ptr: u32) -> u32 {
    let reader = &mut *(reader_ptr as *mut std::io::BufReader<std::fs::File>);
    let mut line = String::new();
    let _ = reader.read_line(&mut line);
    if line.len() == 0 {
        return 1;
    }
    let json_res: serde_json::Result<serde_json::Value> = serde_json::from_str(&line);
    let json = match json_res {
        Ok(json) => json,
        Err(_) => return 1
    };
    // TODO: Fixme, this will read all fields, while we only want a subset. We should get a list of fields to read.
    match json {
        serde_json::Value::Object(map) => {
            for (key, value) in map {
                match value {
                    serde_json::Value::String(s) => {
                        let s = save_wasm_string(s);
                        let record_ptr = record_ptr as *mut u32;
                        *record_ptr = s;
                    }
                    _ => {}
                }
            }
        }
        _ => return 1
    };
    0
}
