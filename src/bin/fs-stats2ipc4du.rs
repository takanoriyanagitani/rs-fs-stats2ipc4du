use std::io;
use std::process::ExitCode;

use rs_fs_stats2ipc4du::Builder;
use rs_fs_stats2ipc4du::Config;

fn io_config() -> impl FnMut() -> Config {
    || Config::default()
}

fn io_main() -> impl FnMut() -> Result<(), io::Error> {
    || {
        let cfg: Config = io_config()();
        Builder::stdin2maps2bat2stdout(cfg.capacity, cfg.string_size_per_item)?;
        Ok(())
    }
}

fn sub() -> Result<(), io::Error> {
    io_main()()
}

fn main() -> ExitCode {
    sub().map(|_| ExitCode::SUCCESS).unwrap_or_else(|e| {
        eprintln!("{e}");

        ExitCode::FAILURE
    })
}
