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
    },

    Live,
}
