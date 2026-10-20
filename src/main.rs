use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{BufRead, BufReader, Write},
    path::Path,
    process::{Command, Stdio},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use nautilus_common::{
    clients::DataClient, live::runner::set_data_event_sender, messages::DataEvent,
    providers::InstrumentProvider,
};
use nautilus_model::{identifiers::ClientId, instruments::Instrument};
use nautilus_network::{retry::RetryConfig, websocket::config::TransportBackend};
use nautilus_polymarket::{
    common::consts::WS_DEFAULT_SUBSCRIPTIONS,
    config::{PolymarketDataClientConfig, PolymarketInstrumentProviderConfig},
    data::PolymarketDataClient,
    filters::EventParamsFilter,
    http::{
        clob::PolymarketClobPublicClient, data_api::PolymarketDataApiHttpClient,
        gamma::PolymarketGammaHttpClient, query::GetGammaEventsParams,
    },
    websocket::pool::PolymarketMarketConnectionPool,
};
use serde_json::{
    Value, json,
    value::{RawValue, to_raw_value},
};
use tabled::{
    builder::Builder,
    grid::config::Offset,
    settings::{Alignment, Style, object::Rows, style::LineChar},
};
use tiny_http::{Header, Response, Server};
use url::Url;

const SOURCE: &str = "https://gamma-api.polymarket.com/events/keyset?limit=1&id=85704&active=true&closed=false&archived=false";
const DATA_DIR: &str = "data";
const MARKET_COUNTS: [usize; 4] = [8, 16, 32, 64];

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    let data = Path::new(DATA_DIR);
    match args.as_slice() {
        [command] if command == "capture" => capture(data),
        [command, count] if command == "serve" => serve(
            data,
            count.to_str().context("invalid market count")?.parse()?,
        ),
        [command] if command == "run" => run(data).await,
        _ => bail!("invalid command"),
    }
}

async fn run(data: &Path) -> Result<()> {
    // Each sample starts with fresh Nautilus state.
    let Some(case) = std::env::var_os("NAUTILUS_BENCH_CASE") else {
        let index: Value = serde_json::from_reader(fs::File::open(data.join("index.json"))?)?;
        if index["source"].as_str() != Some(SOURCE) {
            bail!("fixture is not the event capture; run capture");
        }
        let available = index["markets"].as_u64().context("fixture market count")?;
        let required = MARKET_COUNTS[MARKET_COUNTS.len() - 1];
        if available < required as u64 {
            bail!("fixture has {available} markets; {required} required. Run capture");
        }
        let mut table = Builder::default();
        table.push_record([
            "Markets/event",
            "Instruments",
            "CPU (s)",
            "Peak RSS (MiB)",
            "Event JSON (MiB)",
        ]);
        for (case, markets) in MARKET_COUNTS.iter().enumerate() {
            let output = Command::new(std::env::current_exe()?)
                .arg("run")
                .env("NAUTILUS_BENCH_CASE", case.to_string())
                .stderr(Stdio::inherit())
                .output()?;
            if !output.status.success() {
                bail!("benchmark for {markets} markets failed: {}", output.status);
            }
            let row = std::str::from_utf8(&output.stdout)?;
            table.push_record(row.trim().split(','));
        }
        let mut table = table.build();
        table
            .with(Style::markdown())
            .with(Alignment::right())
            .modify(Rows::one(1), LineChar::horizontal(':', Offset::End(0)));
        println!("{table}");
        return Ok(());
    };
    let case = case
        .to_str()
        .context("invalid benchmark case")?
        .parse::<usize>()?;
    let requested = *MARKET_COUNTS.get(case).context("invalid benchmark case")?;
    let mut server = Command::new(std::env::current_exe()?)
        .arg("serve")
        .arg(requested.to_string())
        .stdout(Stdio::piped())
        .spawn()?;
    let mut endpoint = String::new();
    let result = async {
        BufReader::new(server.stdout.take().context("fixture server stdout")?)
            .read_line(&mut endpoint)?;
        if endpoint.is_empty() {
            bail!("fixture server exited without an endpoint");
        }
        tokio::task::LocalSet::new()
            .run_until(benchmark(data, endpoint.trim(), requested))
            .await
    }
    .await;
    let _ = server.kill();
    server.wait()?;
    result
}

fn capture(data: &Path) -> Result<()> {
    fs::create_dir_all(data)?;
    let captured_at = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let status = Command::new("curl")
        .args(["-L", "--fail", "--silent", "--show-error", "--output"])
        .arg(data.join("event.json"))
        .arg(SOURCE)
        .status()?;
    if !status.success() {
        bail!("Gamma capture failed: {status}");
    }

    // Inspect the saved response; replay preserves raw field values when taking market prefixes.
    let (_, ids) = fixture(data, None)?;
    let markets = ids.len() / 2;
    eprintln!("Captured {markets} usable binary markets in one event");
    fs::write(
        data.join("index.json"),
        serde_json::to_vec_pretty(&json!({
            "source": SOURCE,
            "captured_at_unix": captured_at,
            "markets": markets,
            "events": 1,
        }))?,
    )?;
    let hashes = Command::new("sha256sum")
        .current_dir(data)
        .arg("index.json")
        .arg("event.json")
        .output()?;
    if !hashes.status.success() {
        bail!(
            "sha256sum failed: {}",
            String::from_utf8_lossy(&hashes.stderr)
        );
    }
    fs::write(data.join("SHA256SUMS"), hashes.stdout)?;
    Ok(())
}

fn fixture(data: &Path, limit: Option<usize>) -> Result<(Vec<u8>, BTreeSet<String>)> {
    let mut page: BTreeMap<String, Box<RawValue>> =
        serde_json::from_reader(fs::File::open(data.join("event.json"))?)?;
    let mut events: Vec<BTreeMap<String, Box<RawValue>>> =
        serde_json::from_str(page.remove("events").context("Gamma events array")?.get())?;
    if events.len() != 1 {
        bail!("fixture must contain exactly one event");
    }
    let mut event = events.remove(0);
    let markets: Vec<Box<RawValue>> = serde_json::from_str(
        event
            .remove("markets")
            .context("event markets array")?
            .get(),
    )?;
    let mut kept = Vec::new();
    let mut ids = BTreeSet::new();
    for raw in markets {
        if limit == Some(kept.len()) {
            break;
        }
        let market: Value = serde_json::from_str(raw.get())?;
        if market["active"] != true || market["closed"] == true || market["archived"] == true {
            continue;
        }
        let Some(condition) = market["conditionId"].as_str().filter(|id| !id.is_empty()) else {
            continue;
        };
        let Some(tokens) = market["clobTokenIds"]
            .as_str()
            .and_then(|raw| serde_json::from_str::<[String; 2]>(raw).ok())
        else {
            continue;
        };
        let Some(outcomes) = market["outcomes"]
            .as_str()
            .and_then(|raw| serde_json::from_str::<[String; 2]>(raw).ok())
        else {
            continue;
        };
        if tokens.iter().chain(&outcomes).any(String::is_empty) {
            continue;
        }
        ids.extend(tokens.map(|token| format!("{condition}-{token}.POLYMARKET")));
        kept.push(raw);
    }
    if limit.is_some_and(|requested| kept.len() != requested) || ids.len() != 2 * kept.len() {
        bail!(
            "fixture has {} usable markets with {} unique tokens; requested {limit:?}",
            kept.len(),
            ids.len()
        );
    }
    event.insert("markets".into(), to_raw_value(&kept)?);
    page.insert("events".into(), to_raw_value(&[event])?);
    page.remove("next_cursor");
    Ok((serde_json::to_vec(&page)?, ids))
}

fn serve(data: &Path, requested: usize) -> Result<()> {
    let (body, _) = fixture(data, Some(requested))?;
    let server = Server::http(("127.0.0.1", 0)).map_err(anyhow::Error::msg)?;
    println!("http://{}", server.server_addr());
    std::io::stdout().flush()?;

    for request in server.incoming_requests() {
        let url = Url::parse(&format!("http://localhost{}", request.url()))?;
        let after_cursor = url.query_pairs().any(|(key, _)| key == "after_cursor");
        let (status, response_body) = match (url.path(), after_cursor) {
            ("/events/keyset", false) => (200, body.as_slice()),
            _ => (404, b"fixture response not captured".as_slice()),
        };
        let response = Response::from_data(response_body)
            .with_status_code(status)
            .with_header("Content-Type: application/json".parse::<Header>().unwrap());
        request.respond(response)?;
    }
    Ok(())
}

async fn benchmark(data: &Path, endpoint: &str, requested: usize) -> Result<()> {
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(sender);
    let config = PolymarketDataClientConfig {
        instrument_config: Some(PolymarketInstrumentProviderConfig {
            load_all: true,
            ..Default::default()
        }),
        resolve_poll_enabled: false,
        ..Default::default()
    };
    let gamma = PolymarketGammaHttpClient::new(Some(endpoint.into()), 60, RetryConfig::default())?;
    let clob = PolymarketClobPublicClient::new(Some(endpoint.into()), 60)?;
    let data_api = PolymarketDataApiHttpClient::new(Some(endpoint.into()), 60)?;
    let ws = PolymarketMarketConnectionPool::new(
        None,
        false,
        TransportBackend::default(),
        WS_DEFAULT_SUBSCRIPTIONS,
    );
    let mut client = PolymarketDataClient::new(
        ClientId::from("BOOTSTRAP-BENCH"),
        config,
        gamma,
        clob,
        data_api,
        ws,
    );
    client.add_instrument_filter(Arc::new(EventParamsFilter::new(GetGammaEventsParams {
        active: Some(true),
        closed: Some(false),
        archived: Some(false),
        max_events: Some(1),
        ..Default::default()
    })));
    let tick_output = Command::new("getconf").arg("CLK_TCK").output()?;
    let ticks: f64 = std::str::from_utf8(&tick_output.stdout)?.trim().parse()?;
    let cpu_start = cpu_ticks()?;
    client.connect().await?;
    let cpu = (cpu_ticks()? - cpu_start) as f64 / ticks;
    let status = fs::read_to_string("/proc/self/status")?;
    let peak_rss = status
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))
        .and_then(|value| value.split_whitespace().next())
        .context("Linux peak RSS")?
        .parse::<u64>()?;
    let instruments = client.provider().store().count();
    let actual: BTreeSet<_> = client
        .provider()
        .store()
        .get_all()
        .keys()
        .map(ToString::to_string)
        .collect();
    let (_, expected) = fixture(data, Some(requested))?;
    let mut event_bytes = 0usize;
    let mut event_allocations = BTreeSet::new();
    for instrument in client.provider().store().get_all().values() {
        let event = instrument
            .info()
            .and_then(|info| info.get_str("gamma_event"))
            .context("event discovery did not retain gamma_event")?;
        if event_allocations.insert(event.as_ptr()) {
            event_bytes += event.len();
        }
    }
    // Nautilus buffers instrument events until data clients finish connecting.
    let mut published = 0;
    let mut published_ids = BTreeSet::new();
    while let Ok(event) = receiver.try_recv() {
        if let DataEvent::Instrument(instrument) = event {
            published += 1;
            published_ids.insert(instrument.id().to_string());
        }
    }
    client.disconnect().await?;
    if actual != expected || published_ids != expected || published != expected.len() {
        bail!(
            "instrument IDs differ from fixture: expected={}, stored={instruments}, published={published}",
            expected.len()
        );
    }
    let peak_mib = peak_rss as f64 / 1024.0;
    let event_mib = event_bytes as f64 / (1024.0 * 1024.0);
    println!("{requested},{instruments},{cpu:.2},{peak_mib:.2},{event_mib:.2}");
    Ok(())
}

fn cpu_ticks() -> Result<u64> {
    let stat = fs::read_to_string("/proc/self/stat")?;
    let (_, fields) = stat.rsplit_once(") ").context("Linux process stat")?;
    // The tail begins at field 3; user and system CPU ticks are fields 14 and
    // 15.
    fields
        .split_whitespace()
        .skip(11)
        .take(2)
        .map(|value| value.parse::<u64>().map_err(Into::into))
        .sum()
}
