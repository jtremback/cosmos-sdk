## Slow EndBlocker PoC and Non-Blocking Queries

This PoC demonstrates how a long-running `EndBlocker` (e.g., 5–30s) causes CometBFT RPC `/abci_query` to stall, and how to avoid stalls for application queries by using a separate read-only Query MultiStore.

### TL;DR
- Comet RPC `/abci_query` rides the ABCI connection and is serialized with block execution; if `EndBlocker` or `Commit` is slow, the request waits.
- The SDK supports a distinct read-only query store via `BaseApp.SetQueryMultiStore`. Queries built from this store at the latest committed height do not wait on in-flight block execution.
- This demo exposes:
  - Slow path: Comet RPC `/abci_query` (stalls ~sleep)
  - Fast path: App HTTP endpoint `/height` (uses separate query store; returns immediately)

### How to run
```bash
# Clean ports/state, build, and start with 10s EndBlock sleep
./poc/slowapp/dev.sh 10

# In another terminal, slow path (Comet RPC; stalls)
time curl -s 'http://127.0.0.1:26657/abci_query?path="/store/main/key"&data=0x686569676874&height=0' \
  | jq -r '.result.response.value' | base64 --decode

# Fast path (direct app query; should be immediate)
time curl -s http://127.0.0.1:8080/height
```

### Theory: why RPC stalls and how to avoid it
- **Why stall happens:** CometBFT processes ABCI requests in order. During `FinalizeBlock` → `EndBlock` → `Commit`, Comet issues requests to the app and waits for responses before moving forward. Its own `/abci_query` RPC calls are multiplexed onto ABCI but effectively block behind those in-flight ABCI calls.
- **SDK capability:** `BaseApp.CreateQueryContextWithCheckHeader` selects the query MultiStore `qms` if set via `SetQueryMultiStore`, otherwise the commit MultiStore `cms`. When `qms` is used, it snapshots state at the latest committed version: `qms.CacheMultiStoreWithVersion(latest)`. No writes are taken, so queries are not serialized with block writes.

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

Direct, non-blocking query endpoint (bypasses Comet):

```startLine:216:endLine:270:poc/slowapp/main.go
// startQueryHTTPServer exposes GET /height using CreateQueryContext(0,false)
// which resolves to latest committed height on the query multistore
func startQueryHTTPServer(app *baseapp.BaseApp, keyMain *storetypes.KVStoreKey, addr string) {
    mux := http.NewServeMux()
    mux.HandleFunc("/height", func(w http.ResponseWriter, r *http.Request) {
        ctx, err := app.CreateQueryContext(0, false)
        if err != nil { http.Error(w, err.Error(), http.StatusServiceUnavailable); return }
        store := ctx.KVStore(keyMain)
        val := store.Get([]byte("height"))
        if val == nil { w.WriteHeader(http.StatusNoContent); return }
        _, _ = w.Write(val)
    })
    srv := &http.Server{Addr: addr, Handler: mux}
    go func() { _ = srv.ListenAndServe() }()
}
```

### Observing the difference
- With `EndBlocker` sleeping N seconds, you’ll see:
  - `/abci_query ...` often takes ~N seconds
  - `GET /height` returns immediately

### Notes
- The query store shares the same DB as the main app store and is loaded read-only at the latest committed version.
- This demo uses a minimal in-process CometBFT node and writes only the current height in `BeginBlocker` for visibility.

