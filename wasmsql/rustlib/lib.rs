// Build using `env RUSTFLAGS="-Clink-arg=--no-gc-sections -Clink-arg=--relocatable" cargo build --release --target wasm32-wasi`
// It will include a malloc and free implementation and put them in the wasmsql module, which is nice.

// TODO: Use these not for all "library functions" but just to implement rustlib.

use std::alloc::{Layout};
use std::mem;
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
    let length = *((string_header_ptr +4) as *const u32);
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
