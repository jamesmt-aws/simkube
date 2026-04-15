use std::fs::File;
use std::io::Write;

use clockabilly::prelude::*;
use sk_api::v1::ExportFilters;
use sk_core::prelude::*;
use sk_store::{
    TraceManager,
    TracerConfig,
};

#[derive(clap::Args)]
pub struct Args {
    #[arg(short, long, long_help = "config file specifying resources to snapshot")]
    pub config_file: String,

    #[arg(
        long,
        long_help = "namespaces to exclude from the snapshot",
        value_delimiter = ',',
        default_value = "cert-manager,kube-system,local-path-storage,monitoring,simkube"
    )]
    pub excluded_namespaces: Vec<String>,

    #[arg(
        short,
        long,
        long_help = "location to save exported trace",
        default_value = "trace.out"
    )]
    pub output: String,

    #[arg(
        long,
        long_help = "capture verbatim cluster state into the trace's initial_state field, \
                     instead of recording a deduplicated event stream.  Use this to seed a \
                     replay cluster from a specific captured state."
    )]
    pub seed: bool,

    #[arg(
        long = "cluster-required-label",
        long_help = "restrict captured cluster-scoped objects (Nodes, NodePools, CRs with no \
                     namespace) to those carrying at least one of the given label keys. \
                     Prevents capturing pre-existing infrastructure objects that would be \
                     cascade-deleted on simulation cleanup.  E.g. pass karpenter.sh/nodepool \
                     to include only Karpenter-provisioned Nodes.  Only meaningful with --seed.",
        value_delimiter = ',',
    )]
    pub cluster_required_labels: Vec<String>,
}

pub async fn cmd(args: &Args) -> EmptyResult {
    println!("Reading config from {}...", args.config_file);
    let config = TracerConfig::load(&args.config_file)?;

    println!("Taking snapshot from Kubernetes cluster...");
    let client = kube::Client::try_default().await.expect("failed to create kube client");
    let mut manager = TraceManager::start(client, config).await?;
    manager.wait_ready().await;
    manager.shutdown().await;

    println!("Exporting snapshot data from store...");
    let filters = ExportFilters::new(args.excluded_namespaces.clone(), vec![]);
    let store = manager.get_store();
    let store = store.lock().await;
    let data = if args.seed {
        store.export_seed(&filters, &args.cluster_required_labels)?
    } else {
        if !args.cluster_required_labels.is_empty() {
            println!("warning: --cluster-required-label is ignored without --seed");
        }
        let start_ts = UtcClock.now_ts();
        let end_ts = start_ts + 1;
        store.export(start_ts, end_ts, &filters).await?
    };

    println!("Writing trace file: {}", args.output);
    let mut file = File::create(&args.output)?;
    file.write_all(&data)?;

    println!("Done!");
    Ok(())
}
