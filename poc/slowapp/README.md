## Slow EndBlocker PoC and Non-Blocking Queries

This PoC demonstrates how a long-running `EndBlocker` (e.g., 5–30s) causes CometBFT RPC `/abci_query` to stall, and how to avoid stalls for application queries by using a separate read-only Query MultiStore.

### TL;DR
- Comet RPC `/abci_query` rides the ABCI connection and is serialized with block execution; if `EndBlocker` or `Commit` is slow, the request waits.
- The SDK supports a distinct read-only query store via `BaseApp.SetQueryMultiStore`. Queries built from this store at the latest committed height do not wait on in-flight block execution.
- This demo exposes:
  - Slow path: Comet RPC `/abci_info` (stalls ~sleep)
  - Fast path: App HTTP endpoint `/abci_info` (uses separate query store; returns immediately)

### How to run
```bash
# Clean ports/state, build, and start with 10s EndBlock sleep
./poc/slowapp/dev.sh 10

# In another terminal, slow path (Comet RPC; stalls ~EndBlock sleep)
time curl -s http://127.0.0.1:26657/abci_info | jq .

# Fast path (direct app endpoint; immediate)
time curl -s http://127.0.0.1:8080/abci_info | jq .
```

### Theory: why RPC stalls and how to avoid it
- **Why stall happens:** CometBFT processes ABCI requests in order. During `FinalizeBlock` → `EndBlock` → `Commit`, Comet issues requests to the app and waits for responses before moving forward. Its own `/abci_*` RPC calls are multiplexed onto ABCI but effectively block behind those in-flight ABCI calls.
- **SDK capability:** `BaseApp.CreateQueryContextWithCheckHeader` selects the query MultiStore `qms` if set via `SetQueryMultiStore`, otherwise the commit MultiStore `cms`. When `qms` is used, it snapshots state at the latest committed version. No writes are taken, so queries are not serialized with block writes.

### Is SetQueryMultiStore required for this demo?
- For the fast example `GET /abci_info`: not strictly. It calls `BaseApp.Info()` directly (reads last commit metadata) and bypasses Comet entirely, so it’s already fast.
- For general state queries (gRPC/module APIs using `CreateQueryContext`): recommended. A separate read-only query multistore ensures consistent snapshots at the latest committed height and avoids any interaction with write paths.

### Code excerpts

Set a dedicated Query MultiStore and HTTP route. From `poc/slowapp/main.go`:

```startLine:60:endLine:91:poc/slowapp/main.go
// BaseApp setup and store mounts
app := baseapp.NewBaseApp("slowapp", logger, db, simpleTxDecoder, baseapp.SetChainID("slowapp-local"))
keyMain := storetypes.NewKVStoreKey("main")
app.MountKVStores(map[string]*storetypes.KVStoreKey{
    "main": keyMain,
})

// Load latest version (seals the app)
if err := app.LoadLatestVersion(); err != nil {
    panic(err)
}

// Provide separate query multistore sharing the same DB
qms := rootmulti.NewStore(db, logger, metrics.NewMetrics(nil))
qms.MountStoreWithDB(keyMain, storetypes.StoreTypeIAVL, nil)
if err := qms.LoadLatestVersion(); err != nil {
    panic(err)
}
app.SetQueryMultiStore(qms)
```

Direct, non-blocking ABCI info endpoint (bypasses Comet):

```startLine:300:endLine:338:poc/slowapp/main.go
// GET /abci_info returns BaseApp.Info() fields without going through Comet
mux.HandleFunc("/abci_info", func(w http.ResponseWriter, r *http.Request) {
    ri := &cmtabci.RequestInfo{}
    info, _ := app.Info(ri)
    resp := map[string]any{
        "last_block_height":    info.LastBlockHeight,
        "last_block_app_hash":  hex.EncodeToString(info.LastBlockAppHash),
        "version":              info.Version,
        "app_version":          info.AppVersion,
        "application":          info.Data,
    }
    _ = json.NewEncoder(w).Encode(resp)
})
```

### Observing the difference
- With `EndBlocker` sleeping N seconds, you’ll see:
  - `GET http://127.0.0.1:26657/abci_info` often takes ~N seconds
  - `GET http://127.0.0.1:8080/abci_info` returns immediately

### Notes
- The query store shares the same DB as the main app store and is loaded read-only at the latest committed version.
- This demo uses a minimal in-process CometBFT node and writes only the current height in `BeginBlocker` for visibility.

