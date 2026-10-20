# Nautilus Polymarket event data benchmark

Copying a parent event's full JSON into every outcome instrument makes metadata
storage quadratic in markets per event. The fix shares the complete event JSON
across outcome instruments and cache clones.

Runs the real client's `connect` against one saved Gamma `/events/keyset`
response with increasing markets per event. Each case uses a fresh process
and verifies stored and published instrument IDs. Event JSON counts unique
`gamma_event` allocations in the provider cache; RSS includes all startup
allocations. Requires Linux.

## Baseline

Pinned develop revision: `c6cada8ac8a82b305aa0fbd7948afa504eb40f46`

```sh
cargo run --release -- run
```

| Markets/event | Instruments | CPU (s) | Peak RSS (MiB) | Event JSON (MiB) |
|--------------:|------------:|--------:|---------------:|-----------------:|
|             8 |          16 |    0.00 |          21.89 |             0.48 |
|            16 |          32 |    0.00 |          26.75 |             1.80 |
|            32 |          64 |    0.01 |          42.76 |             6.91 |
|            64 |         128 |    0.01 |         104.53 |            26.99 |

## Proposed version

Measured fix revision: `a9e9a57d398d359d47a732ae92542710671cc6bf`

```sh
cargo run --release --config .cargo/proposed.toml -- run
```

| Markets/event | Instruments | CPU (s) | Peak RSS (MiB) | Event JSON (MiB) |
|--------------:|------------:|--------:|---------------:|-----------------:|
|             8 |          16 |    0.00 |          20.58 |             0.03 |
|            16 |          32 |    0.00 |          20.83 |             0.06 |
|            32 |          64 |    0.00 |          21.39 |             0.11 |
|            64 |         128 |    0.00 |          22.64 |             0.21 |

## Captured data

```sh
cargo run --release -- capture
```

Captured on 2026-10-20 UTC: event 85704, with 71 usable binary markets.
The response is untouched; replay preserves raw field values.
[Capture metadata](data/index.json) and [SHA-256 checksums](data/SHA256SUMS)
are included.
