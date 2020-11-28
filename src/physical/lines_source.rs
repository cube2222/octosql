







#[test]
fn test() {
    let start_time = std::time::Instant::now();
    let path = "goals_big.json";

    let file = File::open(path).expect("failed to open the file");

    let mmap = unsafe { Mmap::map(&file).expect("failed to map the file") };

    let mmap_text = str::from_utf8(&mmap[..]).unwrap();

    let mut line_count: usize = 0;
    let mut line_offsets = Vec::with_capacity(8192);
    for (i, character) in mmap_text.char_indices() {
        if line_offsets.len() == 8192 {
            line_offsets = Vec::with_capacity(8192);
            line_count += 8192;
        }
        if character == '\n' {
            line_offsets.push(i + 1);
        }
    }
    line_count += line_offsets.len();
    dbg!(line_offsets.len());
    dbg!(line_count);
    dbg!(start_time.elapsed());
}