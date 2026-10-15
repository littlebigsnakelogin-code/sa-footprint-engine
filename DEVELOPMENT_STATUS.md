# SA Footprint Engine — Development Status

Last updated: 2026-10-15

## Repository and deployment
- Repository: littlebigsnakelogin-code/sa-footprint-engine
- Main application: app.py
- Stack: Flask/Python, Render, Turso, Binance Futures trade/depth WebSockets.
- Render service: https://sa-footprint-engine.onrender.com
- Symbols: BTCUSDT, ETHUSDT, SOLUSDT, XRPUSDT, AVAXUSDT, LINKUSDT, LTCUSDT.
- Last user-confirmed deployed SHA in conversation: 2ffd3ceb568255449f9a6d37ed179092497889bd.
- Current GitHub main SHA checked on 2026-10-15: ccf7d49e12276108260cbd2ba437e78ea005ae8d.
- GitHub main is exactly 2 commits ahead of the last user-confirmed deployed SHA and 0 commits behind it (GitHub compare verified on 2026-10-15). The optimization PR is based on current main.

## Current optimization PR
- Branch: perf/market-flow-single-pass-main
- PR: https://github.com/littlebigsnakelogin-code/sa-footprint-engine/pull/3
- Latest branch commit before this status document: 0dab73af2a1c2b840ae2f768d119f8da2602b9ad
- Change: in get_market_flow_context(), build 1s, 3s, and 10s candidate-trade windows in one pass over trade history, then use the pre-grouped window lists for the existing aggregation. Existing per-window calculations remain unchanged.
- Added GitHub Actions workflow .github/workflows/python-syntax.yml to run python -m py_compile app.py on PRs.
- Verification: 5,000 randomized comparisons passed for original versus optimized candidate membership in all three windows. GitHub Actions run 38679528499 completed successfully; the Compile app.py step passed under Python 3.11.
- Not yet verified: Render live CPU, queue sizes, and processing-latency improvement. This is not yet a confirmed fix for production overload.
- PR remains open and draft; no merge/deployment has been made from it.

## Current major blockers
1. Collector/processing overload: recent Render observations showed depth queues around 4,737–4,846, trade queue around 4,556, and processing delays around 0.6–6.6 seconds across symbols. Treat as observations from the recent diagnostic window, not guaranteed current values.
2. Intermittent orderbook synchronization: some symbols can remain unsynchronized with empty bid/ask levels while other symbols work; /api/status reporting synchronized:null alone is not enough evidence. This remains a separate major blocker.
3. No live before/after proof: optimize only after controlled deployment, then compare CPU, depth/trade queue backlog, and processing delays against a captured baseline.

## Frozen areas / safety constraints
Do not change without measured evidence: process_trade(), record_trade_for_execution_matching(), matcher core, liquidity finalizer.
Do not mix orderbook synchronization fixes into the market-flow optimization PR.

## Locked liquidity semantics
- Orderflow means executed aggressive trades, not resting book quantity.
- Ask liquidity context is paired with aggressive BUY flow; bid liquidity context with aggressive SELL flow.
- Added resting quantity is not itself a pull.
- LIQUIDITY_PULLED is accounting/evidence, not a definitive spoofing verdict.
- Absorption is separate and requires substantial execution against large resting liquidity with remaining/replenishing liquidity.
- Execution matching remains exact side + exact price index, with the existing timing window and remaining-quantity cap; no nearby-price matching should be introduced without evidence.

## Roadmap / next steps
1. Keep the optimization isolated; inspect the verified PR diff and reconcile branch/deployed-SHA mismatch.
2. If approved for controlled deployment, capture baseline and post-deploy CPU, queue backlog, and processing delays. Do not claim success without measured improvement.
3. Address intermittent per-symbol orderbook synchronization as a separate blocker; proposed snapshot-lock idea remains unverified and must be tested before changing code.
4. Continue the existing roadmap only after blockers are measured: index cleanup audit, Heavy Single-Level V1, absorption, heavy clusters, dynamic distance, contextual pull classification, evidence scoring, Turso, compact APIs, backend freeze, UI, live calibration.

## Workflow
- Work one actionable step at a time.
- Verify source and behavior before proposing edits; do not reverse direction without new evidence.
- No production merge/deploy until relevant checks are complete.
