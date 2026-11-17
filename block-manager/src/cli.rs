// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::LazyLock;

use clap::Parser;
use clap::Subcommand;
use database::sqlite::history_proofs::EventFilter;
use database::sqlite::history_proofs::EventFilters;
use database::sqlite::history_proofs::DEFAULT_BRIDGE_EVENT_DST;
use database::sqlite::history_proofs::DEFAULT_BRIDGE_ROUTING;
use database::sqlite::history_proofs::DEFAULT_VOUCHER_EVENT_DST;
use database::sqlite::history_proofs::DEFAULT_VOUCHER_ROUTING;
use message_router::read_keys_from_file;
use serde::Deserialize;
use transport_layer::HostPort;
use url::Url;

use crate::domain::models::AppConfig;
use crate::domain::models::DEFAULT_BP_PORT;

const DEFAULT_READINESS_MAX_BLOCK_AGE_SECS: u64 = 30;
const MAX_READINESS_MAX_BLOCK_AGE_SECS: u64 = 60;

pub static LONG_VERSION: LazyLock<String> = LazyLock::new(|| {
    format!(
        "
{}
BUILD_GIT_BRANCH={}
BUILD_GIT_COMMIT={}
BUILD_GIT_DATE={}
BUILD_TIME={}",
        env!("CARGO_PKG_VERSION"),
        env!("BUILD_GIT_BRANCH"),
        env!("BUILD_GIT_COMMIT"),
        env!("BUILD_GIT_DATE"),
        env!("BUILD_TIME"),
    )
});

/// Acki-Nacki Block Manager
#[derive(Parser, Debug)]
#[command(author, long_version = &**LONG_VERSION, about, long_about = None)]
pub struct Args {
    /// Subscription to blocks (TODO change type to SocketAddr)
    #[arg(long, env)]
    pub stream_src_url: Option<Url>,

    /// Path to YAML config file with BK endpoints list
    #[arg(long, env = "CONFIG_FILE")]
    pub config: Option<PathBuf>,

    /// REST API endpoint
    #[arg(long, env, default_value = "0.0.0.0:8001")]
    pub rest_api: std::net::SocketAddr,

    /// Directory of the SQLite files (`bm-archive.db`, `bm-history-proofs.db`)
    #[arg(long, env, default_value_t = database::sqlite::sqlite_helper::SQLITE_DATA_DIR.to_string(), global = true)]
    pub sqlite_path: String,

    /// Maximum finalized block age before the readiness endpoint returns 503
    #[arg(
        long,
        env = "READINESS_MAX_BLOCK_AGE_SECS",
        default_value_t = DEFAULT_READINESS_MAX_BLOCK_AGE_SECS,
        value_parser = clap::value_parser!(u64).range(1..=MAX_READINESS_MAX_BLOCK_AGE_SECS)
    )]
    pub readiness_max_block_age_secs: u64,

    /// ClickHouse URL for wallet activity export (e.g. http://localhost:8123)
    /// If not set, ClickHouse export is disabled
    #[arg(long, env = "CLICKHOUSE_URL")]
    pub clickhouse_url: Option<String>,

    /// ClickHouse user (required when clickhouse-url is set)
    #[arg(long, env = "CLICKHOUSE_USER")]
    pub clickhouse_user: Option<String>,

    /// ClickHouse password (required when clickhouse-url is set)
    #[arg(long, env = "CLICKHOUSE_PASSWORD")]
    pub clickhouse_password: Option<String>,

    /// How often to poll the BK `/v2/bk_set` endpoint for the latest BK set snapshot
    #[arg(
        long,
        env = "BK_SET_POLL_INTERVAL_SECS",
        default_value_t = 60,
        value_parser = clap::value_parser!(u64).range(1..=3600)
    )]
    pub bk_set_poll_interval_secs: u64,

    /// Bridge account routing `<dapp_id>::<account_id>` (hex): its
    /// `WithdrawalInitiated` events are kept in the history proofs store
    #[arg(long, env = "BRIDGE_EVENT_ROUTING", default_value = DEFAULT_BRIDGE_ROUTING, global = true)]
    pub bridge_event_routing: String,

    /// `dst` of the bridge `WithdrawalInitiated` events: a 256-bit external address, `:` and
    /// 64 hex digits
    #[arg(long, env = "BRIDGE_EVENT_DST", default_value = DEFAULT_BRIDGE_EVENT_DST, global = true)]
    pub bridge_event_dst: String,

    /// RootPN routing `<dapp_id>::<account_id>` (hex): its `VoucherGenerated` events are kept
    /// in the history proofs store with their skUCommit
    #[arg(long, env = "VOUCHER_EVENT_ROUTING", default_value = DEFAULT_VOUCHER_ROUTING, global = true)]
    pub voucher_event_routing: String,

    /// `dst` of the RootPN `VoucherGenerated` events: a 256-bit external address, `:` and 64
    /// hex digits
    #[arg(long, env = "VOUCHER_EVENT_DST", default_value = DEFAULT_VOUCHER_EVENT_DST, global = true)]
    pub voucher_event_dst: String,

    /// Maintenance command; without one block-manager runs as usual
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Subcommand, Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Work with the history proofs store `<--sqlite-path>/bm-history-proofs.db`
    #[command(subcommand)]
    HistoryProofs(HistoryProofsCommand),
}

/// Exit codes of the `history-proofs` subcommands, shown in their `--help`.
const HISTORY_EXIT_CODES: &str = "Exit codes:
  0  done
  1  incomplete: the data is good but not all there. export: the source misses heights or
     attestations of the range. import: heights or missing attestations left in the range.
     gaps exits 0 or 2 only
  2  any other error: wrong arguments, a missing file or an existing --out file, a foreign
     or older schema, an export file of other event flags or of an export that did not
     finish, an SQLite or IO error (such as a locked file)
  3  bad data, also when data is missing too (an error that ends the run is still 2).
     export: the export file refused some of its rows, or rows of the range failed a check
     other than event_in_tree on the source: they are left out (the file is still
     written).
     import: rejected rows, conflicts, inserted rows that failed a check other than
     event_in_tree (an alert only), rejected or deleted attestations, and rows of
     the range that failed such a check earlier: they stay, so a second import exits 3 again";

#[derive(Subcommand, Debug, Clone, PartialEq, Eq)]
#[command(after_help = HISTORY_EXIT_CODES)]
pub enum HistoryProofsCommand {
    /// Print the first and last stored height, the missing heights between them, the stored
    /// key and rotation blocks older than the last 256 heights without a PRIMARY attestation
    /// (missing attestations), the heights whose rows failed a check other than
    /// event_in_tree (failed checks), the stored heights not verified yet (unverified:
    /// rows waiting for their checks, or with a failed check other than event_in_tree),
    /// and the heights with an event that cannot be proven (unprovable events, an alert only;
    /// in --json: heights, their number; ranges, the newest 100 ranges; total_ranges, the
    /// number of all ranges)
    #[command(after_help = HISTORY_EXIT_CODES)]
    Gaps {
        /// One JSON line on stdout
        #[arg(long)]
        json: bool,
    },
    /// Copy the rows of a height range with their events, BK set updates and
    /// attestations into a new file; prints the source's gaps and missing attestations in it,
    /// and the rows left out because they failed a check other than event_in_tree
    /// here (failed checks)
    #[command(after_help = HISTORY_EXIT_CODES)]
    Export {
        #[arg(long)]
        from_height: u64,
        #[arg(long)]
        to_height: u64,
        /// The export file; must not exist
        #[arg(long)]
        out: PathBuf,
        /// Recorded in the export metadata
        #[arg(long, env = "HOSTNAME")]
        source_id: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Insert the rows and attestations of an export file into this block-manager's store;
    /// prints the gaps and missing attestations left in the range, its rows that failed a
    /// check other than event_in_tree (failed checks), and its heights with an event that
    /// cannot be proven (unprovable events, an alert only, as in gaps)
    #[command(after_help = HISTORY_EXIT_CODES)]
    Import {
        /// The export file
        #[arg(long)]
        from: PathBuf,
        /// Compare every row of the range, not only the missing heights
        #[arg(long)]
        all: bool,
        /// Range start; required for a file without export metadata
        #[arg(long)]
        from_height: Option<u64>,
        /// Range end; required for a file without export metadata
        #[arg(long)]
        to_height: Option<u64>,
        #[arg(long)]
        json: bool,
    },
}

/// An event filter from a routing flag and a dst flag; errors name the flag.
fn filter_from_flags(
    (routing_flag, routing): (&str, &str),
    (dst_flag, dst): (&str, &str),
) -> anyhow::Result<EventFilter> {
    let (routing_dapp_id, routing_account_id) =
        database::sqlite::history_proofs::parse_routing(routing)
            .map_err(|err| anyhow::format_err!("{routing_flag}: {err}"))?;
    let dst_address = database::sqlite::history_proofs::parse_event_dst(dst)
        .map_err(|err| anyhow::format_err!("{dst_flag}: {err}"))?;
    Ok(EventFilter { routing_dapp_id, routing_account_id, dst_address })
}

/// The bridge event filter from `--bridge-event-routing` and `--bridge-event-dst`.
pub fn bridge_filter_from_args(args: &Args) -> anyhow::Result<EventFilter> {
    filter_from_flags(
        ("--bridge-event-routing", &args.bridge_event_routing),
        ("--bridge-event-dst", &args.bridge_event_dst),
    )
}

/// The event filter of each kind: the bridge flags and `--voucher-event-routing`,
/// `--voucher-event-dst`.
pub fn event_filters_from_args(args: &Args) -> anyhow::Result<EventFilters> {
    let voucher = filter_from_flags(
        ("--voucher-event-routing", &args.voucher_event_routing),
        ("--voucher-event-dst", &args.voucher_event_dst),
    )?;
    EventFilters::new(bridge_filter_from_args(args)?, voucher).map_err(|err| {
        anyhow::format_err!(
            "--bridge-event-routing and --bridge-event-dst equal --voucher-event-routing and --voucher-event-dst: {err}"
        )
    })
}

#[derive(Deserialize)]
struct ConfigFile {
    bk_stream_blocks_endpoints: Vec<String>,
    #[serde(default)]
    bk_api_endpoints: Vec<String>,
}

pub struct ParsedConfig {
    pub stream_sockets: Vec<SocketAddr>,
    pub api_endpoints: Vec<HostPort>,
}

pub fn parse_config_file(path: &Path) -> anyhow::Result<ParsedConfig> {
    let contents = std::fs::read_to_string(path)?;
    let config: ConfigFile = serde_yaml::from_str(&contents)?;
    let mut stream_sockets = Vec::new();
    for ep in &config.bk_stream_blocks_endpoints {
        let addr = parse_endpoint(ep)?;
        stream_sockets.push(addr);
    }
    if stream_sockets.is_empty() {
        anyhow::bail!("bk_stream_blocks_endpoints list is empty in {}", path.display());
    }
    let mut api_endpoints = Vec::new();
    for ep in &config.bk_api_endpoints {
        let hp = HostPort::from_str(ep)?;
        api_endpoints.push(hp);
    }
    Ok(ParsedConfig { stream_sockets, api_endpoints })
}

fn parse_endpoint(ep: &str) -> anyhow::Result<SocketAddr> {
    // Try parsing as URL only if it looks like one (contains "://")
    if ep.contains("://") {
        if let Ok(url) = Url::parse(ep) {
            let host =
                url.host_str().ok_or_else(|| anyhow::anyhow!("No host in endpoint: {ep}"))?;
            let port = url
                .port_or_known_default()
                .ok_or_else(|| anyhow::anyhow!("No port in endpoint: {ep}"))?;
            return Ok(parse_socket_address(host, port)?);
        }
    }
    // Try parsing as IP "addr:port"
    if let Ok(addr) = ep.parse::<SocketAddr>() {
        return Ok(addr);
    }
    // Try DNS resolution for "hostname:port" (e.g. docker compose service names)
    let mut addrs = ep.to_socket_addrs()?;
    addrs.next().ok_or_else(|| anyhow::anyhow!("Cannot resolve endpoint: {ep}"))
}

pub fn config_from_args(args: Args) -> anyhow::Result<AppConfig> {
    let event_filters = event_filters_from_args(&args)?;
    let (subscribe_sockets, config_path, config_api_endpoints) =
        if let Some(ref config_path) = args.config {
            let parsed = parse_config_file(config_path)?;
            (parsed.stream_sockets, Some(config_path.clone()), parsed.api_endpoints)
        } else if let Some(ref stream_src_url) = args.stream_src_url {
            let socket = parse_socket_address(
                stream_src_url.host_str().expect("Host required"),
                stream_src_url.port_or_known_default().expect("Port required"),
            )?;
            (vec![socket], None, vec![])
        } else {
            anyhow::bail!("Either --config or --stream-src-url must be provided");
        };

    let default_bp = HostPort::from_str(
        &std::env::var("DEFAULT_BP").expect("DEFAULT_BP environment variable must be set"),
    )?
    .with_default_port(DEFAULT_BP_PORT);
    let bk_api_token =
        std::env::var("BK_API_TOKEN").expect("BK Account API token (BK_API_TOKEN) not found");

    // Use bk_api_endpoints from config if available, otherwise fall back to default_bp
    let bk_api_endpoints = if config_api_endpoints.is_empty() {
        vec![default_bp.clone()]
    } else {
        config_api_endpoints
    };

    Ok(AppConfig {
        default_bp,
        rest_api: args.rest_api,
        subscribe_sockets,
        sqlite_path: args.sqlite_path,
        readiness_max_block_age_secs: args.readiness_max_block_age_secs,
        bk_api_token,
        owner_wallet_pubkey: std::env::var("BM_OWNER_WALLET_PUBKEY").ok(),
        signing_keys: std::env::var("BM_ISSUER_KEYS_FILE")
            .ok()
            .and_then(|path| read_keys_from_file(&path).ok()),
        config_path,
        bk_api_endpoints,
        clickhouse_url: args.clickhouse_url,
        clickhouse_user: args.clickhouse_user,
        clickhouse_password: args.clickhouse_password,
        bk_set_poll_interval: std::time::Duration::from_secs(args.bk_set_poll_interval_secs),
        event_filters,
    })
}

fn parse_socket_address(hostname: &str, port: u16) -> std::io::Result<SocketAddr> {
    let address = (hostname, port);
    // Try to resolve the hostname
    let mut addrs_iter = address.to_socket_addrs()?;
    addrs_iter.next().ok_or_else(|| std::io::Error::other("No address found for hostname"))
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use clap::Parser;

    use super::*;

    #[test]
    fn test_readiness_max_block_age() {
        let default_args = Args::try_parse_from(["block-manager"]).unwrap();
        assert_eq!(default_args.readiness_max_block_age_secs, DEFAULT_READINESS_MAX_BLOCK_AGE_SECS);

        let overridden_args =
            Args::try_parse_from(["block-manager", "--readiness-max-block-age-secs", "45"])
                .unwrap();
        assert_eq!(overridden_args.readiness_max_block_age_secs, 45);

        let max_args = Args::try_parse_from([
            "block-manager",
            "--readiness-max-block-age-secs",
            &MAX_READINESS_MAX_BLOCK_AGE_SECS.to_string(),
        ])
        .unwrap();
        assert_eq!(max_args.readiness_max_block_age_secs, MAX_READINESS_MAX_BLOCK_AGE_SECS);

        assert!(
            Args::try_parse_from(["block-manager", "--readiness-max-block-age-secs", "0"]).is_err()
        );
        assert!(Args::try_parse_from([
            "block-manager",
            "--readiness-max-block-age-secs",
            &(MAX_READINESS_MAX_BLOCK_AGE_SECS + 1).to_string(),
        ])
        .is_err());
    }

    #[test]
    fn test_bk_set_poll_interval() {
        let default_args = Args::try_parse_from(["block-manager"]).unwrap();
        assert_eq!(default_args.bk_set_poll_interval_secs, 60);
        let args =
            Args::try_parse_from(["block-manager", "--bk-set-poll-interval-secs", "15"]).unwrap();
        assert_eq!(args.bk_set_poll_interval_secs, 15);
        assert!(
            Args::try_parse_from(["block-manager", "--bk-set-poll-interval-secs", "0"]).is_err()
        );
        assert!(
            Args::try_parse_from(["block-manager", "--bk-set-poll-interval-secs", "3601"]).is_err()
        );
    }

    #[test]
    fn bridge_flags_default() {
        // Env vars are read by every parse; keep the env-dependent checks in one test.
        std::env::remove_var("BRIDGE_EVENT_ROUTING");
        std::env::remove_var("BRIDGE_EVENT_DST");
        let args = Args::try_parse_from(["block-manager"]).unwrap();
        assert_eq!(args.bridge_event_routing, DEFAULT_BRIDGE_ROUTING);
        assert_eq!(args.bridge_event_dst, DEFAULT_BRIDGE_EVENT_DST);
        assert_eq!(bridge_filter_from_args(&args).unwrap(), EventFilter::withdrawal_default());

        let routing = format!("{}::{}", "00".repeat(32), "1b".repeat(32));
        let dst = format!(":{:064x}", 0x26b);
        std::env::set_var("BRIDGE_EVENT_ROUTING", &routing);
        std::env::set_var("BRIDGE_EVENT_DST", &dst);
        let from_env = Args::try_parse_from(["block-manager"]);
        std::env::remove_var("BRIDGE_EVENT_ROUTING");
        std::env::remove_var("BRIDGE_EVENT_DST");
        let filter = bridge_filter_from_args(&from_env.unwrap()).unwrap();
        assert_eq!(filter.routing_account_id, [0x1b; 32]);
        assert_eq!(filter.dst(), dst);

        // The voucher flags in the same test: env vars are process-wide.
        std::env::remove_var("VOUCHER_EVENT_ROUTING");
        std::env::remove_var("VOUCHER_EVENT_DST");
        let args = Args::try_parse_from(["block-manager"]).unwrap();
        assert_eq!(args.voucher_event_routing, DEFAULT_VOUCHER_ROUTING);
        assert_eq!(args.voucher_event_dst, DEFAULT_VOUCHER_EVENT_DST);
        assert_eq!(event_filters_from_args(&args).unwrap(), EventFilters::default());
        let routing = format!("{}::{}", "04".repeat(32), "1d".repeat(32));
        let dst = format!(":{:064x}", 0x88);
        std::env::set_var("VOUCHER_EVENT_ROUTING", &routing);
        std::env::set_var("VOUCHER_EVENT_DST", &dst);
        let from_env = Args::try_parse_from(["block-manager"]);
        std::env::remove_var("VOUCHER_EVENT_ROUTING");
        std::env::remove_var("VOUCHER_EVENT_DST");
        let filters = event_filters_from_args(&from_env.unwrap()).unwrap();
        assert_eq!(filters.voucher.routing_account_id, [0x1d; 32]);
        assert_eq!(filters.voucher.dst(), dst);
        assert_eq!(filters.withdrawal, EventFilter::withdrawal_default());
    }

    #[test]
    fn voucher_flags_from_cli() {
        let routing = format!("{}::{}", "04".repeat(32), "1e".repeat(32));
        let args = Args::try_parse_from([
            "block-manager",
            "--voucher-event-routing",
            &routing,
            "--voucher-event-dst",
            &format!(":{}", "AB".repeat(32)),
        ])
        .unwrap();
        let filters = event_filters_from_args(&args).unwrap();
        assert_eq!(filters.voucher.routing_account_id, [0x1e; 32]);
        assert_eq!(filters.voucher.dst(), format!(":{}", "ab".repeat(32)));
        for (flag, value) in [("--voucher-event-dst", ":87"), ("--voucher-event-routing", "abc")] {
            let args = Args::try_parse_from(["block-manager", flag, value]).unwrap();
            let err = event_filters_from_args(&args).unwrap_err().to_string();
            assert!(err.contains(flag), "{err}");
        }
    }

    #[test]
    fn voucher_flags_are_global() {
        let dst = format!(":{:064x}", 0x89);
        let args = Args::try_parse_from([
            "block-manager",
            "history-proofs",
            "gaps",
            "--voucher-event-dst",
            &dst,
        ])
        .unwrap();
        assert_eq!(args.voucher_event_dst, dst);
    }

    #[test]
    fn same_pair_for_both_kinds_is_refused() {
        // Both pairs on the command line: another test sets the env vars.
        let args = Args::try_parse_from([
            "block-manager",
            "--bridge-event-routing",
            DEFAULT_VOUCHER_ROUTING,
            "--bridge-event-dst",
            DEFAULT_VOUCHER_EVENT_DST,
            "--voucher-event-routing",
            DEFAULT_VOUCHER_ROUTING,
            "--voucher-event-dst",
            DEFAULT_VOUCHER_EVENT_DST,
        ])
        .unwrap();
        let err = event_filters_from_args(&args).unwrap_err().to_string();
        assert!(err.contains("--bridge-event-routing"), "{err}");
        assert!(err.contains("--voucher-event-routing"), "{err}");
    }

    #[test]
    fn bridge_flags_from_cli() {
        let routing = format!("{}::{}", "04".repeat(32), "1c".repeat(32));
        let args = Args::try_parse_from([
            "block-manager",
            "--bridge-event-routing",
            &routing,
            "--bridge-event-dst",
            &format!(":{}", "01".repeat(32)),
        ])
        .unwrap();
        let filter = bridge_filter_from_args(&args).unwrap();
        assert_eq!(filter.routing_dapp_id, [0x04; 32]);
        assert_eq!(filter.routing_account_id, [0x1c; 32]);
        assert_eq!(filter.src(), format!("0:{}", "1c".repeat(32)));
        assert_eq!(filter.dst_address, [0x01; 32]);
    }

    #[test]
    fn bridge_event_dst_is_normalized_or_refused() {
        let upper = format!(":{}26A", "0".repeat(61));
        let args = Args::try_parse_from(["block-manager", "--bridge-event-dst", &upper]).unwrap();
        assert_eq!(bridge_filter_from_args(&args).unwrap().dst(), DEFAULT_BRIDGE_EVENT_DST);
        let internal = format!("0:{:064x}", 0x26a);
        let args =
            Args::try_parse_from(["block-manager", "--bridge-event-dst", &internal]).unwrap();
        let err = bridge_filter_from_args(&args).unwrap_err().to_string();
        assert!(err.contains("--bridge-event-dst"), "{err}");
        let short = Args::try_parse_from(["block-manager", "--bridge-event-dst", ":26a"]).unwrap();
        let err = bridge_filter_from_args(&short).unwrap_err().to_string();
        assert!(err.contains("--bridge-event-dst"), "{err}");
    }

    #[test]
    fn bad_bridge_routing_is_an_error() {
        let args =
            Args::try_parse_from(["block-manager", "--bridge-event-routing", "abc"]).unwrap();
        let err = bridge_filter_from_args(&args).unwrap_err().to_string();
        assert!(err.contains("--bridge-event-routing"), "{err}");
    }

    #[test]
    fn history_proofs_help_shows_exit_codes() {
        use clap::CommandFactory;

        let mut args = Args::command();
        let history = args.find_subcommand_mut("history-proofs").unwrap();
        for name in ["gaps", "export", "import"] {
            let help = history.find_subcommand_mut(name).unwrap().render_help().to_string();
            assert!(help.contains("Exit codes:"), "{name}: {help}");
            assert!(help.contains("missing attestations"), "{name}: {help}");
        }
    }

    #[test]
    fn no_subcommand_runs_bm() {
        assert_eq!(Args::try_parse_from(["block-manager"]).unwrap().command, None);
    }

    #[test]
    fn cli_parses_history_subcommands() {
        let gaps =
            Args::try_parse_from(["block-manager", "history-proofs", "gaps", "--json"]).unwrap();
        assert_eq!(
            gaps.command,
            Some(Command::HistoryProofs(HistoryProofsCommand::Gaps { json: true }))
        );
        let export = Args::try_parse_from([
            "block-manager",
            "history-proofs",
            "export",
            "--from-height",
            "10",
            "--to-height",
            "20",
            "--out",
            "x.db",
            "--source-id",
            "bm1",
        ])
        .unwrap();
        assert_eq!(
            export.command,
            Some(Command::HistoryProofs(HistoryProofsCommand::Export {
                from_height: 10,
                to_height: 20,
                out: "x.db".into(),
                source_id: Some("bm1".to_string()),
                json: false,
            }))
        );
        let import = Args::try_parse_from([
            "block-manager",
            "history-proofs",
            "import",
            "--from",
            "x.db",
            "--all",
        ])
        .unwrap();
        assert_eq!(
            import.command,
            Some(Command::HistoryProofs(HistoryProofsCommand::Import {
                from: "x.db".into(),
                all: true,
                from_height: None,
                to_height: None,
                json: false,
            }))
        );
    }

    #[test]
    fn subcommand_accepts_global_flags_after_it() {
        let args = Args::try_parse_from([
            "block-manager",
            "history-proofs",
            "gaps",
            "--sqlite-path",
            "/data/x",
        ])
        .unwrap();
        assert_eq!(args.sqlite_path, "/data/x");
        let routing = format!("{}::{}", "00".repeat(32), "1b".repeat(32));
        let args = Args::try_parse_from([
            "block-manager",
            "history-proofs",
            "import",
            "--from",
            "x.db",
            "--bridge-event-routing",
            &routing,
        ])
        .unwrap();
        assert_eq!(args.bridge_event_routing, routing);
    }

    #[test]
    fn test_parse_config_file() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        writeln!(tmp, "bk_stream_blocks_endpoints:\n  - 127.0.0.1:12000\n  - 127.0.0.1:12001")
            .unwrap();
        let parsed = parse_config_file(tmp.path()).unwrap();
        assert_eq!(parsed.stream_sockets.len(), 2);
        assert_eq!(parsed.stream_sockets[0].port(), 12000);
        assert_eq!(parsed.stream_sockets[1].port(), 12001);
        assert!(parsed.api_endpoints.is_empty());
    }

    #[test]
    fn test_parse_config_file_with_api_endpoints() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        writeln!(
            tmp,
            "bk_stream_blocks_endpoints:\n  - 127.0.0.1:12000\nbk_api_endpoints:\n  - node0:8600\n  - node1:8600"
        )
        .unwrap();
        let parsed = parse_config_file(tmp.path()).unwrap();
        assert_eq!(parsed.stream_sockets.len(), 1);
        assert_eq!(parsed.api_endpoints.len(), 2);
        assert_eq!(parsed.api_endpoints[0].to_string(), "node0:8600");
        assert_eq!(parsed.api_endpoints[1].to_string(), "node1:8600");
    }

    #[test]
    fn test_parse_config_file_empty_list() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        writeln!(tmp, "bk_stream_blocks_endpoints: []").unwrap();
        assert!(parse_config_file(tmp.path()).is_err());
    }
}
