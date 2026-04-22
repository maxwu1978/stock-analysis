# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Personal quant research + paper-trading toolbox spanning A-shares, US equities, US options, crypto, and US futures. Live system has three moving parts on macOS: Python analysis scripts, a local Futu OpenD bridge (127.0.0.1:11111) for US market data/options, and three `launchd` jobs that push HTML snippets to GitHub Pages.

Primary language of comments, prints, and docs is **Chinese (Simplified)**. Keep new output in Chinese to match.

See `PROJECT_STATUS.md` for the current research state, open positions, and known experiment outcomes (e.g. BTC reversal strategy shelved at 37.5% win rate; the one validated edge is `BUY_CALL @ strong_asym_oversold` on US tech at ~67.9%). Consult it before starting any research task — many directions have already been tried and ruled out.

## Hard Safety Rules (non-negotiable)

These are invariants for this codebase. Violating them risks real money even when the file says "SIMULATE".

- **Never run `buy` / `sell` / `limit_sell` / `stop_sell` / `unlock_trade` / `cancel` via `trade_futu_sim.py`.** These are for the human only. Claude's job is to *print* the exact command string for the user to paste — the advisor scripts already do this. Read-only subcommands (`balance`, `positions`, `orders`, `history`) are fine.
- **Do not change `TRD_ENV` in `trade_futu_sim.py`.** It is hardcoded to `TrdEnv.SIMULATE` at module top (line ~36) and must not be read from args/env/config. Any code path that could flip it to `REAL` is a regression.
- **Do not commit or push real-account data.** `real_position_observer.py` writes `real_position_section.html` *locally only*; the launchd job deliberately excludes it from `git add`. Only `option_section.html` (simulate) is published.
- Order-size caps live in `trade_futu_sim.py` (`MAX_ORDER_VALUE_USD=50000`, `MAX_ORDER_VALUE_HKD=400000`, `MAX_OPTION_VALUE_USD=5000`). Do not raise them without explicit user instruction.
- Trading password never belongs in code — it is entered in the Futu OpenD GUI.

## Environment & Common Commands

- Python 3.12, dependencies in `requirements.txt` (key: `futu-api`, `yfinance`, `akshare`, `pywencai`, `flask`). Venv expected at `./venv/`; launchd jobs invoke `./venv/bin/python` explicitly.
- Most scripts assume Futu OpenD is running and logged in (`lsof -iTCP:11111 -sTCP:LISTEN`). Without it, anything using `fetch_futu` / `trade_futu_sim` / `option_*` will fail. `fetch_us`, `fetch_data` (A-shares via Sina/Tencent), `backtest*` and `system_test.py` work without OpenD.
- No test framework, no linter, no build step. "Tests" = `python system_test.py` (statistical validation of the probability model against baselines) and the `backtest*.py` family (directional backtests).

Typical entry points:

```bash
# Publish / refresh the public page (docs/index.html)
python generate_page.py

# Daily option strategy advisor (prints recommendations + trade commands)
python option_fractal_advisor.py tech_plus    # watchlist key from WATCHLISTS dict

# Monitor current simulate-account option positions (PnL / Greeks / DTE)
python option_monitor.py                      # --quiet for cron, --market-open-only to skip off-hours

# Read-only account queries
python trade_futu_sim.py balance|positions|orders|history

# Validation & backtests
python system_test.py                         # 7-test model validation (baselines, OOS, regimes, shuffle, costs)
python backtest.py        # A-shares v1
python backtest_v2.py     # A-shares, current model
python backtest_us.py     # US equities
python option_advisor_backtest.py             # option strategy directional backtest
python signal_hit_rate.py                     # actual signal vs. realized yfinance moves

# Local Flask preview of the A-share section
python web.py

# launchd management (macOS host only)
./install_launchd.sh install|uninstall|status|test
```

There is no single-test runner — `system_test.py` executes tests 1–7 sequentially over `STOCKS` + `US_STOCKS`. To run a subset, edit the `main()` loop or import `test_N_*` functions from a REPL.

## Architecture

Flow is strictly layered; each layer only imports from layers above it.

**Data layer** — one fetcher per market, no analysis logic:
- `fetch_data.py` → A-shares (Sina realtime, Tencent historical K-line). Hardcoded 5-stock universe in `STOCKS`.
- `fetch_us.py` → US equities via `yfinance`. Hardcoded 6-ticker universe in `US_STOCKS`.
- `fetch_futu.py` → Futu OpenD: realtime quotes, K-lines, ATM option chain, positions. **Read-only by design — do not add place_order/modify_order here.**
- `fetch_wencai.py` → 同花顺问财 natural-language stock screener (`pywencai`).
- `fetch_futures_yf.py` → US futures daily bars via yfinance (research only; no execution path).
- `fundamental.py` → A-share financial statements (akshare).

**Analysis layer** — pure pandas, no I/O:
- `indicators.py` → ~40 factors: classic TA (MA/MACD/RSI/BOLL/ADX/ROC), fat-tail precursors (BOLL width, vol compression, vol surge, ADX acceleration, 20d kurtosis), MF-DFA spectrum features (`mfdfa_width_120d`, `mfdfa_alpha0_120d`, `hq2_120d`, `mfdfa_asym_120d`), plus amihud illiquidity and overnight gap return. `compute_all(df, fund_df=None)` is the single entry; `summarize(df)` emits the display row.
- `fractal_survey.py` → MF-DFA spectrum kernel (`mfdfa_spectrum`) consumed by every fractal-related script (`option_fractal_advisor`, `industry_fractal`, `crypto_fractal_survey`, `temporal_fractal`, `us_fractal`, …). Q list `[-4,-2,2,4]`, window 120d.
- `probability.py` / `probability_us.py` → **IC-adaptive weighting model**. Rolling 120-day Spearman IC of each factor vs. forward 5-day return is used as both weight *and* sign, so the model auto-switches between momentum and mean-reversion regimes per stock without human priors. `FACTOR_COLS` at top defines the enabled factor set. Adding a factor = add to `indicators.py` + add symbol to `FACTOR_COLS`.
- `iv_rank.py` → IV rank via realized-vol proxy, accumulates real IV into `iv_history.csv` toward the 252-day threshold for switching to true IV Rank (per `PROJECT_STATUS.md`, target ~July).
- `macro_events.py` → Fed/CPI/earnings calendar + VIX gate used by advisor scripts to suppress signals around risk events.

**Strategy layer** — analysis → actionable recommendation, still no order placement:
- `option_fractal_advisor.py` → main advisor. Maps fractal + RSI + MA20-deviation features into 7 option scenarios; emits concrete contract codes and the **exact `trade_futu_sim.py buy ... --confirm` command string** for the user to copy. `WATCHLISTS` dict at top defines named pools (`tech`, `tech_plus`, `etf`, etc.). `BTC_ETF_UNDERLYING` forces IBIT/FBTC/BITB to use BTC-USD klines for the fractal, since the ETFs have <2y of data.
- `option_straddle_advisor.py` → IV-rank + Δα (fractal spectrum width) dual-filter for straddle candidates.
- `option_monitor.py` → periodic job: parses option codes (`US.NVDA260424P200000` → underlying/expiry/type/strike), pulls live quotes + Greeks, aggregates straddle pairs, computes PnL / theta-decay / break-even distance / DTE. Writes `option_status.log`, `option_status_latest.txt`, `option_section.html`, and fires macOS notifications.
- `real_position_observer.py` → same shape as `option_monitor`, but against the REAL account and writes to a file that is never pushed.
- `option_advisor_backtest.py`, `signal_hit_rate.py` → retrospective evaluation of the advisor's actual output vs. yfinance realized prices.
- `btc_*`, `crypto_fractal_survey.py`, `industry_*`, `futures_*`, `energy_reverse.py`, `fat_tail_study.py`, `fractal_strategy.py`, `temporal_fractal.py` → research scripts. Several (BTC daily/4h, crypto reversal) are **documented failures**; see `PROJECT_STATUS.md` and inline warnings before reviving them.

**Execution layer** — `trade_futu_sim.py` only. Every write path requires the literal CLI flag `--confirm`; without it, the function prints "未加 --confirm, 拒绝下单" and returns. All actions (success + failure) are appended to `trade_sim_log.csv`.

**Presentation layer**:
- `generate_page.py` → renders `docs/index.html` (the GitHub Pages site). It composes sections 01–08 itself and injects two pre-rendered fragments it does not own: `option_section.html` (from `option_monitor.py`) and, locally only, `real_position_section.html` (from `real_position_observer.py`).
- `analyze.py` / `analyze_us.py` → Markdown CLI reports (same data as the page, terminal output).
- `web.py` → local Flask preview of A-share analysis with background cache warmup.

**Automation (macOS launchd)** — three jobs, all defined in `com.maxwu.*.plist` + installed via `install_launchd.sh`. Each shell wrapper aborts early unless OpenD port 11111 is listening:
- `com.maxwu.fractal-advisor` — weekdays 15:00 & 22:00 CEST → `run_advisor_daily.sh` → `option_fractal_advisor.py tech_plus` → appends to `advisor_history.log`.
- `com.maxwu.option-monitor` — hourly → `run_option_monitor.sh` → `option_monitor.py` + `real_position_observer.py` → regenerates page → commits & pushes only if `option_section.html` or `real_position_section.html` changed (but `real_position_section.html` is in `.gitignore`, so only the simulate section reaches GitHub).
- `com.maxwu.auto-hedge` — weekdays 15:30 CEST → `auto_hedge_daily.sh` → reads open option longs, places +30% DAY take-profit limit sells via `trade_futu_sim.py limit_sell ... --confirm`. Idempotent: skips codes that already have an active SELL order. This is the *one* place in the codebase where `--confirm` runs unattended; it only works because it's a narrow, bounded action on the simulate account.

Plists hardcode `WorkingDirectory=/Volumes/MaxRelocated/主力分析` — that is the host's real path; on other machines edit the plists before `./install_launchd.sh install`.

**GitHub Actions** — `.github/workflows/update-page.yml` runs `python generate_page.py` on weekdays 15:35 Beijing time (07:35 UTC). This regenerates the A-share + US sections only; it does not have OpenD access, so option sections are refreshed from the local launchd job, not from CI.

## Conventions to follow

- Column names in DataFrames and printed headers are Chinese (`最新价`, `涨跌幅`, `成交额`, …). Stay consistent.
- Option codes follow Futu's format: `US.AAPL260424P200000` = `US.AAPL` / `2026-04-24` / `PUT` / strike `200.000`. Use `option_monitor.parse_option_code` rather than re-parsing.
- Market prefixes: `US.` / `HK.` / `SH.` / `SZ.`. A-share fetchers use the bare 6-digit code and translate via `_sina_symbol` (`3xxxxx`/`0xxxxx` → `sz`, else `sh`).
- Adding a stock to the public page = add to `fetch_data.STOCKS` or `fetch_us.US_STOCKS` *and* to the matching `*_SIGNAL_RELIABILITY` dict in `generate_page.py` / `analyze.py`.
- Adding a stock to the advisor's rotation = add to a `WATCHLISTS` entry in `option_fractal_advisor.py`.
- Adding a factor = implement in `indicators.py`, register in `probability.FACTOR_COLS` (and `probability_us.py` if US). The IC-adaptive model auto-weights it; no threshold tuning required.
- Never silently catch and discard errors in new data-layer code — the fetchers' pattern is `print(f"  [!] ... 失败: {e}")` and returning empty so downstream scripts can continue with partial data.
