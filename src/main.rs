fn main() {
    if let Err(error) = fungos_records::run_cli() {
        eprintln!("records: {error:#}");
        std::process::exit(1);
    }
}
