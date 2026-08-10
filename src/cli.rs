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

        #[arg(long, default_value_t = 1)]
        repeat: u32,

        #[arg(long, default_value_t = false)]
        resolve: bool,
    },

    Verify {
        #[arg(long)]
        path: String,
    },

    VerifyRange {
        #[arg(long)]
        from: u64,

        #[arg(long)]
        to: u64,
    },

    Backfill {
        #[arg(long)]
        from: u64,

        #[arg(long)]
        to: u64,
    },

    Recover,

    Live,
}
