package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	abcisrv "github.com/cometbft/cometbft/abci/server"
	cmtcfg "github.com/cometbft/cometbft/config"
	cmtlog "github.com/cometbft/cometbft/libs/log"
	cmtnode "github.com/cometbft/cometbft/node"
	p2p "github.com/cometbft/cometbft/p2p"
	pvm "github.com/cometbft/cometbft/privval"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	cmtproxy "github.com/cometbft/cometbft/proxy"
	cmttypes "github.com/cometbft/cometbft/types"
	dbm "github.com/cosmos/cosmos-db"

	"cosmossdk.io/log"
	// optional imports kept minimal for PoC
	"cosmossdk.io/store/metrics"
	"cosmossdk.io/store/rootmulti"
	storetypes "cosmossdk.io/store/types"

	"github.com/cosmos/cosmos-sdk/baseapp"
	serversdk "github.com/cosmos/cosmos-sdk/server"
	sdk "github.com/cosmos/cosmos-sdk/types"
)

// slowEndBlocker sleeps for the configured duration on every EndBlock.
func slowEndBlocker(ctx sdk.Context) (sdk.EndBlock, error) {
    sleep := ctx.Value("sleep").(time.Duration)
    ctx.Logger().Info("Slow EndBlock sleeping", "duration", sleep)
    time.Sleep(sleep)
    return sdk.EndBlock{}, nil
}

// simpleTxDecoder rejects all txs; we don't need tx handling for this PoC.
func simpleTxDecoder(_ []byte) (sdk.Tx, error) { return nil, fmt.Errorf("no txs supported") }

func main() {
    // Flags
    var (
        mode         string
        abciAddr     string
        sleepSeconds int
        httpAddr     string
    )
    flag.StringVar(&mode, "mode", "inproc", "Mode: inproc|socket")
    flag.StringVar(&abciAddr, "abci", "tcp://127.0.0.1:26658", "ABCI listen address (socket mode)")
    flag.IntVar(&sleepSeconds, "sleep", 30, "EndBlock sleep seconds")
    flag.StringVar(&httpAddr, "http", "127.0.0.1:8080", "HTTP query server address")
    flag.Parse()

    sleep := time.Duration(sleepSeconds) * time.Second

    // Logger
    logger := log.NewLogger(os.Stdout)

    // In-memory DB
    db := dbm.NewMemDB()

    // BaseApp setup
    app := baseapp.NewBaseApp("slowapp", logger, db, simpleTxDecoder, baseapp.SetChainID("slowapp-local"))

    // Create and mount a single persistent KV store
    keyMain := storetypes.NewKVStoreKey("main")
    app.MountKVStores(map[string]*storetypes.KVStoreKey{
        "main": keyMain,
    })

    // Set blockers BEFORE sealing (LoadLatestVersion seals the app)
    app.SetBeginBlocker(func(ctx sdk.Context) (sdk.BeginBlock, error) {
        // write current height at key "height"
        store := ctx.KVStore(keyMain)
        if store != nil {
            store.Set([]byte("height"), []byte(fmt.Sprintf("%d", ctx.BlockHeight())))
        }
        return sdk.BeginBlock{}, nil
    })
    app.SetEndBlocker(func(ctx sdk.Context) (sdk.EndBlock, error) {
        // pass sleep via context to avoid global var
        ctx = ctx.WithValue("sleep", sleep)
        return slowEndBlocker(ctx)
    })
    // Provide a minimal in-memory ParamStore so consensus params can be stored on InitChain
    app.SetParamStore(&inMemoryParamStore{})

    // Load latest version (seals the app)
    if err := app.LoadLatestVersion(); err != nil {
        panic(err)
    }

    // Set up a separate query MultiStore sharing the same DB, mounted read-only
    qms := rootmulti.NewStore(db, logger, metrics.NewMetrics(nil))
    qms.MountStoreWithDB(keyMain, storetypes.StoreTypeIAVL, nil)
    if err := qms.LoadLatestVersion(); err != nil {
        panic(err)
    }
    app.SetQueryMultiStore(qms)

    if mode == "socket" {
        // ABCI socket server
        cmtWrapped := serversdk.NewCometABCIWrapper(app)
        srv, err := abcisrv.NewServer(abciAddr, "socket", cmtWrapped)
        if err != nil {
            panic(err)
        }
        if err := srv.Start(); err != nil {
            panic(err)
        }
        logger.Info("slowapp ABCI server started", "addr", abciAddr, "sleep", sleep.String())

        // Wait for signal
        sigCh := make(chan os.Signal, 1)
        signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
        <-sigCh
        _ = srv.Stop()
        logger.Info("slowapp ABCI server stopped")
        return
    }
    // In-process CometBFT node setup for a single-binary demo
    // Start a lightweight HTTP server that queries app state directly (bypasses Comet)
    go startQueryHTTPServer(app, keyMain, httpAddr)

    if err := runInProcessComet(app, logger); err != nil {
        panic(err)
    }
}

// runInProcessComet boots a minimal CometBFT node that connects to the app in-process.
func runInProcessComet(app *baseapp.BaseApp, logger log.Logger) error {
    // Prepare a temporary root dir under current working dir
    cwd, _ := os.Getwd()
    root := filepath.Join(cwd, "poc", "slowapp", ".tm")
    if err := os.MkdirAll(root, 0o755); err != nil {
        return err
    }

    cfg := cmtcfg.DefaultConfig()
    cfg.SetRoot(root)
    // Enable RPC on default address for convenience
    cfg.RPC.ListenAddress = "tcp://127.0.0.1:26657"
    // Keep P2P but it's a single node setup
    cfg.P2P.ListenAddress = "tcp://127.0.0.1:26656"

    // Ensure genesis exists
    genFile := cfg.GenesisFile()
    if _, err := os.Stat(genFile); os.IsNotExist(err) {
        // load/generate priv validator to get pubkey (ensure both key and state dirs exist)
        if err := os.MkdirAll(filepath.Dir(cfg.PrivValidatorKeyFile()), 0o755); err != nil {
            return err
        }
        if err := os.MkdirAll(filepath.Dir(cfg.PrivValidatorStateFile()), 0o755); err != nil {
            return err
        }
        pv := pvm.LoadOrGenFilePV(cfg.PrivValidatorKeyFile(), cfg.PrivValidatorStateFile())
        pub, err := pv.GetPubKey()
        if err != nil {
            return err
        }
        gen := &cmttypes.GenesisDoc{
            ChainID:         "slowapp-local",
            GenesisTime:     time.Now(),
            ConsensusParams: cmttypes.DefaultConsensusParams(),
            Validators: []cmttypes.GenesisValidator{{
                Address: pub.Address(),
                PubKey:  pub,
                Power:   1,
                Name:    "self",
            }},
            AppHash:  []byte{},
            AppState: []byte("{}"),
        }
        if err := gen.SaveAs(genFile); err != nil {
            return err
        }
    }

    // Load or generate keys
    // Ensure data dir exists for privval state file
    if err := os.MkdirAll(filepath.Dir(cfg.PrivValidatorStateFile()), 0o755); err != nil {
        return err
    }
    pv := pvm.LoadOrGenFilePV(cfg.PrivValidatorKeyFile(), cfg.PrivValidatorStateFile())
    nodeKey, err := p2p.LoadOrGenNodeKey(cfg.NodeKeyFile())
    if err != nil {
        return err
    }

    // Wrap app for Comet ABCI
    cmtApp := serversdk.NewCometABCIWrapper(app)

    // Provide genesis doc provider
    genDocProvider := func() (*cmttypes.GenesisDoc, error) {
        return cmttypes.GenesisDocFromFile(genFile)
    }

    tmLogger := cmtlog.NewTMLogger(os.Stdout)
    node, err := cmtnode.NewNode(
        cfg,
        pv,
        nodeKey,
        cmtproxy.NewLocalClientCreator(cmtApp),
        genDocProvider,
        cmtcfg.DefaultDBProvider,
        cmtnode.DefaultMetricsProvider(cfg.Instrumentation),
        tmLogger,
    )
    if err != nil {
        return err
    }
    if err := node.Start(); err != nil {
        return err
    }
    logger.Info("in-process CometBFT node started", "rpc", cfg.RPC.ListenAddress)

    // Wait for signal and stop node
    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
    <-sigCh
    _ = node.Stop()
    return nil
}

// inMemoryParamStore is a trivial ParamStore implementation held in memory for the PoC.
type inMemoryParamStore struct{}

// Get returns params or an empty one if unset.
func (s *inMemoryParamStore) Get(_ context.Context) (cmtproto.ConsensusParams, error) { return cmtproto.ConsensusParams{}, nil }

func (s *inMemoryParamStore) Has(_ context.Context) (bool, error) { return false, nil }

func (s *inMemoryParamStore) Set(_ context.Context, _ cmtproto.ConsensusParams) error { return nil }

// startQueryHTTPServer starts an HTTP server exposing a direct query endpoint that does not
// go through CometBFT. It uses BaseApp's CreateQueryContext (with the separate qms) and should
// return immediately even during EndBlock/Commit.
func startQueryHTTPServer(app *baseapp.BaseApp, keyMain *storetypes.KVStoreKey, addr string) {
    mux := http.NewServeMux()
    mux.HandleFunc("/height", func(w http.ResponseWriter, r *http.Request) {
        // height=0 means latest committed
        ctx, err := app.CreateQueryContext(0, false)
        if err != nil {
            http.Error(w, err.Error(), http.StatusServiceUnavailable)
            return
        }
        store := ctx.KVStore(keyMain)
        if store == nil {
            http.Error(w, "store not found", http.StatusNotFound)
            return
        }
        val := store.Get([]byte("height"))
        if val == nil {
            w.WriteHeader(http.StatusNoContent)
            return
        }
        _, _ = w.Write(val)
    })
    srv := &http.Server{Addr: addr, Handler: mux}
    go func() { _ = srv.ListenAndServe() }()
}

