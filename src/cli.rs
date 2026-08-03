use clap::{Parser, Subcommand};

#[derive(Parser)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    Capture {
        #[arg(long, default_value_t = 5)]
        minutes: u64,
    },

    Replay {
        #[arg(long)]
        path: String,

        // How many times to feed the file through the pipeline.
        // 0 means forever, which is what the soak test uses.
        #[arg(long, default_value_t = 1)]
        repeat: u32,

        // Off by default so plain replay stays network-free, which is what
        // the README promises and what the tests rely on. Turning it on lets
        // the pool resolver make RPC calls, which is the only way orient()
        // ever runs on a replayed file.
        #[arg(long, default_value_t = false)]
        resolve: bool,
    },

    Live,
}
