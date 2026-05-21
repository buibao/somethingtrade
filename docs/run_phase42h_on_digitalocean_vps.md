# Run Phase 4.2H On A DigitalOcean VPS

This guide prepares a DigitalOcean VPS to run the exact Phase 4.2H hot-path environment latency benchmark. It does not create droplets, use DigitalOcean API tokens, handle secrets, or move the project to Phase 5.

## Recommended Droplet

- Provider: DigitalOcean
- Region: Singapore first
- OS: Ubuntu 24.04 LTS recommended, Ubuntu 22.04 LTS acceptable if Python 3.12 is available
- Size: at least 2 vCPU / 4 GB RAM
- Disk: SSD/NVMe
- Network: no VPN or proxy

Phase 4.2H keeps `max_future_gap_ms=100` and the strict 100ms observability requirement as a hard gate.

## SSH And Firewall Safety

- Use SSH keys, not password login.
- Keep port 22 open only to your own IP when practical.
- Do not paste DigitalOcean API tokens or exchange secrets onto the droplet.
- The benchmark only needs outbound HTTPS/WebSocket access to Binance and inbound SSH for you.
- Destroy the droplet after the test if you no longer need it.

## Clone And Checkout

On the VPS:

```bash
sudo apt-get update
sudo apt-get install -y git ca-certificates
git clone <YOUR_REPO_URL> somethingtrade
cd somethingtrade
git checkout <COMMIT_OR_BRANCH_TO_AUDIT>
git rev-parse HEAD
```

Use the exact commit you want audited. Include that commit hash when sending results.

## Setup

Run the setup script from the repo root:

```bash
bash scripts/setup_phase42h_vps_ubuntu.sh
```

Optional chrony package install:

```bash
bash scripts/setup_phase42h_vps_ubuntu.sh --install-chrony
```

The script creates `.venv`, installs dependencies, runs a quick import check, and writes:

```text
data/debug/phase_4_2h_vps_setup_report.txt
```

It does not run the 30-minute benchmark.

Manual equivalent, if you need to inspect each step:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
python -m pip install -e ".[dev]" || python -m pip install -r requirements.txt || python -m pip install -e .
```

## Preflight

The benchmark wrapper runs preflight automatically, but you can run it directly:

```bash
source .venv/bin/activate
python -X utf8 scripts/run_phase42h_hotpath_environment_latency.py \
  --preflight-only \
  --environment-name vps_singapore_do \
  --environment-region SG \
  --machine-profile "2vCPU-4GB-Ubuntu-DO" \
  --network-notes "DigitalOcean Singapore preflight" \
  --run-mode vps_preflight
```

Preflight writes:

```text
data/debug/phase_4_2h_vps_preflight_report.json
```

## 2-Minute Smoke

Smoke mode is only a VPS wiring check. It is not final latency evidence.

```bash
bash scripts/run_phase42h_vps_benchmark.sh \
  --mode smoke \
  --environment-name vps_singapore_do \
  --environment-region SG \
  --machine-profile "2vCPU-4GB-Ubuntu-DO" \
  --network-notes "DigitalOcean Singapore smoke" \
  --duration-sec 120
```

## 30-Minute Final Benchmark

Final mode hard-fails if `--duration-sec` is below `1800`.

```bash
bash scripts/run_phase42h_vps_benchmark.sh \
  --mode final \
  --environment-name vps_singapore_do \
  --environment-region SG \
  --machine-profile "2vCPU-4GB-Ubuntu-DO" \
  --network-notes "DigitalOcean Singapore final 30m" \
  --duration-sec 1800
```

Expected bundle path after a pass:

```text
phase_4_2h_hotpath_environment_latency_bundle.zip
```

Expected bundle path after a fail:

```text
phase_4_2h_hotpath_environment_latency_fail_audit_bundle.zip
```

## Verify Bundle And Checksum

The wrapper calls the collector automatically. To run it again:

```bash
bash scripts/collect_phase42h_vps_bundle.sh
cat phase_4_2h_bundle_sha256.txt
ls -lh phase_4_2h_hotpath_environment_latency_bundle.zip phase_4_2h_hotpath_environment_latency_fail_audit_bundle.zip 2>/dev/null || true
```

The checksum file contains the filename, SHA256, byte size, UTC timestamp, and absolute path.

## Download To Windows

From PowerShell on your Windows/local machine:

```powershell
scp root@<DROPLET_IP>:/root/somethingtrade/phase_4_2h_hotpath_environment_latency_bundle.zip .
scp root@<DROPLET_IP>:/root/somethingtrade/phase_4_2h_hotpath_environment_latency_fail_audit_bundle.zip .
scp root@<DROPLET_IP>:/root/somethingtrade/phase_4_2h_bundle_sha256.txt .
```

One of the two bundle downloads may fail because only pass or fail is expected. That is fine.

If you cloned somewhere other than `/root/somethingtrade`, adjust the remote path.

## Files To Send For Audit

Send:

```text
1. source zip for the exact commit used
2. phase_4_2h_hotpath_environment_latency_bundle.zip
   or phase_4_2h_hotpath_environment_latency_fail_audit_bundle.zip
3. phase_4_2h_bundle_sha256.txt
```

If comparing environments, send each environment bundle separately, for example local Vietnam and DigitalOcean Singapore.

## Cleanup

After you have downloaded the audit files, destroy the droplet if it is no longer needed so it stops billing.
