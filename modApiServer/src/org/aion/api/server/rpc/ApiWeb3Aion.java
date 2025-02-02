package org.aion.api.server.rpc;

import static java.util.stream.Collectors.toList;
import static org.aion.util.bytes.ByteUtil.EMPTY_BYTE_ARRAY;
import static org.aion.util.conversions.Hex.toHexString;
import static org.aion.util.types.HexConvert.hexStringToBytes;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import org.aion.api.server.ApiAion;
import org.aion.api.server.ApiTxResponse;
import org.aion.api.server.types.ArgFltr;
import org.aion.api.server.types.ArgTxCall;
import org.aion.api.server.types.Blk;
import org.aion.api.server.types.CompiledContr;
import org.aion.api.server.types.Evt;
import org.aion.api.server.types.Fltr;
import org.aion.api.server.types.FltrBlk;
import org.aion.api.server.types.FltrLg;
import org.aion.api.server.types.FltrTx;
import org.aion.api.server.types.NumericalValue;
import org.aion.api.server.types.SyncInfo;
import org.aion.api.server.types.Tx;
import org.aion.api.server.types.TxRecpt;
import org.aion.base.AionTransaction;
import org.aion.base.TransactionTypes;
import org.aion.crypto.ECKey;
import org.aion.crypto.HashUtil;
import org.aion.evtmgr.IEventMgr;
import org.aion.evtmgr.IHandler;
import org.aion.evtmgr.impl.callback.EventCallback;
import org.aion.evtmgr.impl.evt.EventTx;
import org.aion.zero.impl.keystore.Keystore;
import org.aion.mcf.blockchain.Block;
import org.aion.mcf.blockchain.BlockHeader;
import org.aion.mcf.config.CfgApi;
import org.aion.mcf.config.CfgApiNrg;
import org.aion.mcf.config.CfgApiRpc;
import org.aion.mcf.config.CfgApiZmq;
import org.aion.mcf.config.CfgNet;
import org.aion.mcf.config.CfgNetP2p;
import org.aion.mcf.config.CfgSsl;
import org.aion.mcf.config.CfgSync;
import org.aion.mcf.config.CfgTx;
import org.aion.base.AccountState;
import org.aion.zero.impl.core.ImportResult;
import org.aion.mcf.db.Repository;
import org.aion.util.types.DataWord;
import org.aion.types.AionAddress;
import org.aion.types.Log;
import org.aion.util.bytes.ByteUtil;
import org.aion.util.string.StringUtils;
import org.aion.util.types.AddressUtils;
import org.aion.util.types.ByteArrayWrapper;
import org.aion.util.types.Hash256;
import org.aion.zero.impl.blockchain.AionBlockchainImpl;
import org.aion.zero.impl.types.BlockContext;
import org.aion.zero.impl.Version;
import org.aion.zero.impl.blockchain.AionImpl;
import org.aion.zero.impl.blockchain.IAionChain;
import org.aion.zero.impl.config.CfgAion;
import org.aion.zero.impl.config.CfgConsensusPow;
import org.aion.zero.impl.config.CfgEnergyStrategy;
import org.aion.zero.impl.db.AionBlockStore;
import org.aion.zero.impl.db.AionRepositoryImpl;
import org.aion.zero.impl.sync.NodeWrapper;
import org.aion.zero.impl.sync.PeerState;
import org.aion.zero.impl.types.AionBlock;
import org.aion.zero.impl.types.AionBlockSummary;
import org.aion.zero.impl.types.AionTxInfo;
import org.aion.base.AionTxReceipt;
import org.apache.commons.collections4.map.LRUMap;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * @author chris lin, ali sharif TODO: make implementation pass all spec tests:
 *     https://github.com/ethereum/rpc-tests
 */
@SuppressWarnings({"Duplicates", "WeakerAccess"})
public class ApiWeb3Aion extends ApiAion {

    private static final int SYNC_TOLERANCE = 1;

    private final int OPS_RECENT_ENTITY_COUNT = 32;
    private final int OPS_RECENT_ENTITY_CACHE_TIME_SECONDS = 4;

    private final int STRATUM_RECENT_BLK_COUNT = 128;
    private final int STRATUM_BLKTIME_INCLUDED_COUNT = 32;
    private final int STRATUM_CACHE_TIME_SECONDS = 15;
    // TODO: Verify if need to use a concurrent map; locking may allow for use of a simple map
    private HashMap<ByteArrayWrapper, AionBlock> templateMap;
    private ReadWriteLock templateMapLock;
    private IEventMgr evtMgr;
    // doesn't need to be protected for concurrent access, since only one write in the constructor.
    private boolean isFilterEnabled;

    private boolean isSeedMode;

    private final long BEST_PENDING_BLOCK = -1L;

    private final LoadingCache<Integer, ChainHeadView> CachedRecentEntities;
    private final LoadingCache<String, MinerStatsView> MinerStats;

    protected void onBlock(AionBlockSummary cbs) {
        if (isFilterEnabled) {
            installedFilters
                    .keySet()
                    .forEach(
                            (k) -> {
                                Fltr f = installedFilters.get(k);
                                if (f.isExpired()) {
                                    LOG.debug("<Filter: expired, key={}>", k);
                                    installedFilters.remove(k);
                                } else if (f.onBlock(cbs)) {
                                    LOG.debug(
                                            "<Filter: append, onBlock type={} blk#={}>",
                                            f.getType().name(),
                                            cbs.getBlock().getNumber());
                                }
                            });
        }
    }

    protected void pendingTxReceived(AionTransaction _tx) {
        if (isFilterEnabled) {
            // not absolutely neccessary to do eviction on installedFilters here, since we're doing
            // it already
            // in the onBlock event. eviction done here "just in case ..."
            installedFilters
                    .keySet()
                    .forEach(
                            (k) -> {
                                Fltr f = installedFilters.get(k);
                                if (f.isExpired()) {
                                    LOG.debug("<filter expired, key={}>", k);
                                    installedFilters.remove(k);
                                } else if (f.onTransaction(_tx)) {
                                    LOG.info(
                                            "<filter append, onPendingTransaction fltrSize={} type={} txHash={}>",
                                            f.getSize(),
                                            f.getType().name(),
                                            StringUtils.toJsonHex(_tx.getTransactionHash()));
                                }
                            });
        }
    }

    protected void pendingTxUpdate(AionTxReceipt _txRcpt, EventTx.STATE _state) {
        // commenting this out because of lack support for old web3 client that we are using
        // TODO: re-enable this when we upgrade our web3 client
        /*
        if (isFilterEnabled) {
            ByteArrayWrapper txHashW = new ByteArrayWrapper(((AionTxReceipt) _txRcpt).getTransaction().getHash());
            if (_state.isPending() || _state == EventTx.STATE.DROPPED0) {
                pendingReceipts.put(txHashW, (AionTxReceipt) _txRcpt);
            } else {
                pendingReceipts.remove(txHashW);
            }
        }
        */
    }

    private final LoadingCache<ByteArrayWrapper, Block> blockCache;
    private static final int BLOCK_CACHE_SIZE = 1000;

    public ApiWeb3Aion(final IAionChain _ac) {
        super(_ac);
        pendingReceipts = Collections.synchronizedMap(new LRUMap<>(FLTRS_MAX, 100));
        templateMap = new HashMap<>();
        templateMapLock = new ReentrantReadWriteLock();
        isFilterEnabled = CfgAion.inst().getApi().getRpc().isFiltersEnabled();
        isSeedMode = CfgAion.inst().getConsensus().isSeed();

        initNrgOracle(_ac);

        if (isFilterEnabled) {
            evtMgr = this.ac.getAionHub().getEventMgr();

            startES("EpWeb3");

            // Fill data on block and transaction events into the filters and pending receipts
            IHandler blkHr = evtMgr.getHandler(IHandler.TYPE.BLOCK0.getValue());
            if (blkHr != null) {
                blkHr.eventCallback(new EventCallback(ees, LOG));
            }

            IHandler txHr = evtMgr.getHandler(IHandler.TYPE.TX0.getValue());
            if (txHr != null) {
                txHr.eventCallback(new EventCallback(ees, LOG));
            }
        }

        // ops-related endpoints
        CachedRecentEntities =
                Caffeine.newBuilder()
                        .maximumSize(1)
                        .refreshAfterWrite(OPS_RECENT_ENTITY_CACHE_TIME_SECONDS, TimeUnit.SECONDS)
                        .build(
                                new CacheLoader<>() {
                                    public ChainHeadView load(Integer key) { // no checked exception
                                        return new ChainHeadView(OPS_RECENT_ENTITY_COUNT).update();
                                    }

                                    // reload() executed asynchronously using
                                    // ForkJoinPool.commonPool()
                                    public ChainHeadView reload(
                                            final Integer key, ChainHeadView prev) {
                                        return new ChainHeadView(prev).update();
                                    }
                                });

        //noinspection NullableProblems
        blockCache =
                Caffeine.newBuilder()
                        .maximumSize(BLOCK_CACHE_SIZE)
                        .build(
                                new CacheLoader<>() {
                                    public Block load(ByteArrayWrapper blockHash) {
                                        LOG.debug(
                                                "<rpc-server blockCache miss for "
                                                        + blockHash.toString()
                                                        + " >");
                                        return getBlockByHash(blockHash.getData());
                                    }
                                });

        MinerStats =
                Caffeine.newBuilder()
                        .maximumSize(1)
                        .refreshAfterWrite(STRATUM_CACHE_TIME_SECONDS, TimeUnit.SECONDS)
                        .build(
                                new CacheLoader<>() {
                                    public MinerStatsView load(String key) { // no checked exception
                                        AionAddress miner = AddressUtils.wrapAddress(key);
                                        return new MinerStatsView(
                                                        STRATUM_RECENT_BLK_COUNT,
                                                        miner.toByteArray())
                                                .update();
                                    }

                                    // reload() executed asynchronously using
                                    // ForkJoinPool.commonPool()
                                    public MinerStatsView reload(
                                            final String key, MinerStatsView prev) {
                                        return new MinerStatsView(prev).update();
                                    }
                                });
    }

    private void destroyCaches() {
        CachedRecentEntities.invalidateAll();
        MinerStats.invalidateAll();
        blockCache.invalidateAll();
    }

    // --------------------------------------------------------------------
    // Mining Pool
    // --------------------------------------------------------------------

    /* Return a reference to the AIONBlock without converting values to hex
     * Requied for the mining pool implementation
     */
    private Block getBlockRaw(int bn) {
        // long bn = this.parseBnOrId(_bnOrId);
        Block nb = this.ac.getBlockchain().getBlockByNumber(bn);

        if (nb == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("<get-block-raw bn={} err=not-found>", bn);
            }
            return null;
        } else {
            return nb;
        }
    }

    // --------------------------------------------------------------------
    // Ethereum-Compliant JSON RPC Specification Implementation
    // --------------------------------------------------------------------

    public RpcMsg web3_clientVersion() {
        return new RpcMsg(this.clientVersion);
    }

    public RpcMsg web3_sha3(Object _params) {
        String _data;
        if (_params instanceof JSONArray) {
            _data = ((JSONArray) _params).get(0) + "";
        } else if (_params instanceof JSONObject) {
            _data = ((JSONObject) _params).get("data") + "";
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        return new RpcMsg(
                StringUtils.toJsonHex(HashUtil.keccak256(ByteUtil.hexStringToBytes(_data))));
    }

    public RpcMsg net_version() {
        return new RpcMsg(chainId());
    }

    public RpcMsg net_peerCount() {
        return new RpcMsg(peerCount());
    }

    public RpcMsg net_listening() {
        // currently, p2p manager is always listening for peers and is active
        return new RpcMsg(true);
    }

    public RpcMsg eth_protocolVersion() {
        return new RpcMsg(p2pProtocolVersion());
    }

    /**
     * Returns an {@link RpcMsg} containing 'false' if the node is done syncing to the network
     * (since a node is never really 'done' syncing, we consider a node to be done if it is within
     * {@value SYNC_TOLERANCE} blocks of the network best block number).
     *
     * <p>Otherwise, if the sync is still syncing, a {@link RpcMsg} is returned with some basic
     * statistics relating to the sync: the block number the node was at when syncing began; the
     * current block number of the node; the current block number of the network.
     *
     * <p>If either the local block number or network block number cannot be determined, an {@link
     * RpcMsg} is returned with an {@link RpcError#INTERNAL_ERROR} code and its 'result' will be a
     * more descriptive string indicating what went wrong.
     *
     * @return the syncing statistics.
     */
    public RpcMsg eth_syncing() {
        Optional<Long> localBestBlockNumber = this.ac.getLocalBestBlockNumber();
        Optional<Long> networkBestBlockNumber = this.ac.getNetworkBestBlockNumber();

        // Check that we actually have real values in our hands.
        if (!localBestBlockNumber.isPresent()) {
            return new RpcMsg(
                    "Unable to determine the local node's best block number!",
                    RpcError.INTERNAL_ERROR);
        }
        if (!networkBestBlockNumber.isPresent()) {
            return new RpcMsg(
                    "Unable to determine the network's best block number!",
                    RpcError.INTERNAL_ERROR);
        }

        SyncInfo syncInfo = getSyncInfo(localBestBlockNumber.get(), networkBestBlockNumber.get());

        return (syncInfo.done) ? new RpcMsg(false) : new RpcMsg(syncInfoToJson(syncInfo));
    }

    private JSONObject syncInfoToJson(SyncInfo syncInfo) {
        JSONObject syncInfoAsJson = new JSONObject();
        syncInfoAsJson.put(
                "startingBlock", new NumericalValue(syncInfo.chainStartingBlkNumber).toHexString());
        syncInfoAsJson.put(
                "currentBlock", new NumericalValue(syncInfo.chainBestBlkNumber).toHexString());
        syncInfoAsJson.put(
                "highestBlock", new NumericalValue(syncInfo.networkBestBlkNumber).toHexString());
        return syncInfoAsJson;
    }

    public RpcMsg eth_coinbase() {
        return new RpcMsg(getCoinbase());
    }

    public RpcMsg eth_mining() {
        return new RpcMsg(isMining());
    }

    public RpcMsg eth_hashrate() {
        return new RpcMsg(getHashrate());
    }

    public RpcMsg eth_submitHashrate(Object _params) {
        String _hashrate;
        String _clientId;
        if (_params instanceof JSONArray) {
            _hashrate = ((JSONArray) _params).get(0) + "";
            _clientId = ((JSONArray) _params).get(1) + "";
        } else if (_params instanceof JSONObject) {
            _hashrate = ((JSONObject) _params).get("hashrate") + "";
            _clientId = ((JSONObject) _params).get("clientId") + "";
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        return new RpcMsg(setReportedHashrate(_hashrate, _clientId));
    }

    public RpcMsg eth_gasPrice() {
        return new RpcMsg(StringUtils.toJsonHex(getRecommendedNrgPrice()));
    }

    public RpcMsg eth_accounts() {
        return new RpcMsg(new JSONArray(getAccounts()));
    }

    public RpcMsg eth_blockNumber() {
        return new RpcMsg(getBestBlock().getNumber());
    }

    public RpcMsg eth_getBalance(Object _params) {
        String _address;
        Object _bnOrId;

        if (_params instanceof JSONArray) {
            _address = ((JSONArray) _params).get(0) + "";
            _bnOrId = ((JSONArray) _params).opt(1);
        } else if (_params instanceof JSONObject) {
            _address = ((JSONObject) _params).get("address") + "";
            _bnOrId = ((JSONObject) _params).opt("block") + "";
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        AionAddress address = AddressUtils.wrapAddress(_address);

        String bnOrId = "latest";
        if (!JSONObject.NULL.equals(_bnOrId)) {
            bnOrId = _bnOrId + "";
        }

        Repository repo = getRepoByJsonBlockId(bnOrId);
        if (repo == null) // invalid bnOrId
        {
            return new RpcMsg(
                    null,
                    RpcError.EXECUTION_ERROR,
                    "Block not found for id / block number: "
                            + bnOrId
                            + ". "
                            + "State may have been pruned; please check your db pruning settings in the configuration file.");
        }

        BigInteger balance = repo.getBalance(address);
        return new RpcMsg(StringUtils.toJsonHex(balance));
    }

    public RpcMsg eth_getStorageAt(Object _params) {
        String _address;
        String _index;
        Object _bnOrId;
        if (_params instanceof JSONArray) {
            _address = ((JSONArray) _params).get(0) + "";
            _index = ((JSONArray) _params).get(1) + "";
            _bnOrId = ((JSONArray) _params).opt(2);
        } else if (_params instanceof JSONObject) {
            _address = ((JSONObject) _params).get("address") + "";
            _index = ((JSONObject) _params).get("index") + "";
            _bnOrId = ((JSONObject) _params).opt("block");
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        AionAddress address = AddressUtils.wrapAddress(_address);

        String bnOrId = "latest";
        if (!JSONObject.NULL.equals(_bnOrId)) {
            bnOrId = _bnOrId + "";
        }

        DataWord key;

        try {
            key = new DataWord(ByteUtil.hexStringToBytes(_index));
        } catch (Exception e) {
            // invalid key
            LOG.debug("eth_getStorageAt: invalid storageIndex. Must be <= 16 bytes.");
            return new RpcMsg(
                    null, RpcError.INVALID_PARAMS, "Invalid storageIndex. Must be <= 16 bytes.");
        }

        Repository repo = getRepoByJsonBlockId(bnOrId);
        if (repo == null) // invalid bnOrId
        {
            return new RpcMsg(
                    null,
                    RpcError.EXECUTION_ERROR,
                    "Block not found for id / block number: "
                            + bnOrId
                            + ". "
                            + "State may have been pruned; please check your db pruning settings in the configuration file.");
        }

        ByteArrayWrapper storageValue = repo.getStorageValue(address, key.toWrapper());
        if (storageValue != null) {
            return new RpcMsg(StringUtils.toJsonHex(storageValue.getData()));
        } else {
            return new RpcMsg(null, RpcError.EXECUTION_ERROR, "Storage value not found");
        }
    }

    public RpcMsg eth_getTransactionCount(Object _params) {
        String _address;
        Object _bnOrId;
        if (_params instanceof JSONArray) {
            _address = ((JSONArray) _params).get(0) + "";
            _bnOrId = ((JSONArray) _params).opt(1);
        } else if (_params instanceof JSONObject) {
            _address = ((JSONObject) _params).get("address") + "";
            _bnOrId = ((JSONObject) _params).opt("block");
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        AionAddress address = AddressUtils.wrapAddress(_address);

        String bnOrId = "latest";
        if (!JSONObject.NULL.equals(_bnOrId)) {
            bnOrId = _bnOrId + "";
        }

        Repository repo = getRepoByJsonBlockId(bnOrId);
        if (repo == null) // invalid bnOrId
        {
            return new RpcMsg(
                    null,
                    RpcError.EXECUTION_ERROR,
                    "Block not found for id / block number: "
                            + bnOrId
                            + ". "
                            + "State may have been pruned; please check your db pruning settings in the configuration file.");
        }

        return new RpcMsg(StringUtils.toJsonHex(repo.getNonce(address)));
    }

    public RpcMsg eth_getBlockTransactionCountByHash(Object _params) {
        String _hash;
        if (_params instanceof JSONArray) {
            _hash = ((JSONArray) _params).get(0) + "";
        } else if (_params instanceof JSONObject) {
            _hash = ((JSONObject) _params).get("hash") + "";
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        byte[] hash = ByteUtil.hexStringToBytes(_hash);
        Block b = this.ac.getBlockchain().getBlockByHash(hash);
        if (b == null) {
            return new RpcMsg(null, RpcError.EXECUTION_ERROR, "Block not found.");
        }

        long n = b.getTransactionsList().size();
        return new RpcMsg(StringUtils.toJsonHex(n));
    }

    public RpcMsg eth_getBlockTransactionCountByNumber(Object _params) {
        String _bnOrId;
        if (_params instanceof JSONArray) {
            _bnOrId = ((JSONArray) _params).get(0) + "";
        } else if (_params instanceof JSONObject) {
            _bnOrId = ((JSONObject) _params).get("block") + "";
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        Long bn = parseBnOrId(_bnOrId);
        if (bn == null) {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid block number.");
        }

        // pending transactions
        if (bn == BEST_PENDING_BLOCK) {
            long pendingTxCount = this.ac.getAionHub().getPendingState().getPendingTxSize();
            return new RpcMsg(StringUtils.toJsonHex(pendingTxCount));
        }

        Block b = this.ac.getBlockchain().getBlockByNumber(bn);
        if (b == null) {
            return new RpcMsg(null, RpcError.EXECUTION_ERROR, "Block not found.");
        }

        List<AionTransaction> list = b.getTransactionsList();

        long n = list.size();
        return new RpcMsg(StringUtils.toJsonHex(n));
    }

    public RpcMsg eth_getCode(Object _params) {
        String _address;
        Object _bnOrId;
        if (_params instanceof JSONArray) {
            _address = ((JSONArray) _params).get(0) + "";
            _bnOrId = ((JSONArray) _params).opt(1);
        } else if (_params instanceof JSONObject) {
            _address = ((JSONObject) _params).get("address") + "";
            _bnOrId = ((JSONObject) _params).opt("block");
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        AionAddress address = AddressUtils.wrapAddress(_address);

        String bnOrId = "latest";
        if (!JSONObject.NULL.equals(_bnOrId)) {
            bnOrId = _bnOrId + "";
        }

        Repository repo = getRepoByJsonBlockId(bnOrId);
        if (repo == null) // invalid bnOrId
        {
            return new RpcMsg(
                    null,
                    RpcError.EXECUTION_ERROR,
                    "Block not found for id / block number: "
                            + bnOrId
                            + ". "
                            + "State may have been pruned; please check your db pruning settings in the configuration file.");
        }

        byte[] code = repo.getCode(address);
        return new RpcMsg(StringUtils.toJsonHex(code));
    }

    public RpcMsg eth_sign(Object _params) {
        String _address;
        String _message;
        if (_params instanceof JSONArray) {
            _address = ((JSONArray) _params).get(0) + "";
            _message = ((JSONArray) _params).get(1) + "";
        } else if (_params instanceof JSONObject) {
            _address = ((JSONObject) _params).get("address") + "";
            _message = ((JSONObject) _params).get("message") + "";
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        AionAddress address = AddressUtils.wrapAddress(_address);
        ECKey key = getAccountKey(address.toString());
        if (key == null) {
            return new RpcMsg(null, RpcError.NOT_ALLOWED, "Account not unlocked.");
        }

        // Message starts with Unicode Character 'END OF MEDIUM' (U+0019)
        String message = "\u0019Aion Signed Message:\n" + _message.length() + _message;
        byte[] messageHash = HashUtil.keccak256(message.getBytes());

        return new RpcMsg(StringUtils.toJsonHex(key.sign(messageHash).getSignature()));
    }

    /**
     * Sign a transaction. This account needs to be unlocked.
     *
     * @param _params
     * @return
     */
    public RpcMsg eth_signTransaction(Object _params) {
        JSONObject _tx;
        // Address to sign with
        String _address = null;
        if (_params instanceof JSONArray) {
            _tx = ((JSONArray) _params).getJSONObject(0);

            if (((JSONArray) _params).length() > 1) _address = ((JSONArray) _params).get(1) + "";
        } else if (_params instanceof JSONObject) {
            _tx = ((JSONObject) _params).getJSONObject("transaction");
            _address = ((JSONObject) _params).get("address") + "";
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        ArgTxCall txParams = ArgTxCall.fromJSON(_tx, getRecommendedNrgPrice());
        if (txParams == null)
            return new RpcMsg(
                    null, RpcError.INVALID_PARAMS, "Please check your transaction object.");

        AionTransaction tx = signTransaction(txParams, _address);
        if (tx != null) {
            JSONObject obj = new JSONObject();
            obj.put("raw", StringUtils.toJsonHex(tx.getEncoded()));

            JSONObject txObj = new JSONObject();
            txObj.put("nonce", StringUtils.toJsonHex(tx.getNonce()));
            txObj.put("gasPrice", StringUtils.toJsonHex(tx.getEnergyPrice()));
            txObj.put("nrgPrice", StringUtils.toJsonHex(tx.getEnergyPrice()));
            txObj.put("gas", StringUtils.toJsonHex(tx.getEnergyLimit()));
            txObj.put("nrg", StringUtils.toJsonHex(tx.getEnergyLimit()));
            txObj.put(
                    "to",
                    StringUtils.toJsonHex(
                            tx.getDestinationAddress() == null
                                    ? EMPTY_BYTE_ARRAY
                                    : tx.getDestinationAddress().toByteArray()));
            txObj.put("value", StringUtils.toJsonHex(tx.getValue()));
            txObj.put("input", StringUtils.toJsonHex(tx.getData()));
            txObj.put("hash", StringUtils.toJsonHex(tx.getTransactionHash()));

            obj.put("tx", txObj);
            return new RpcMsg(obj);
        } else {
            if (LOG.isDebugEnabled()) LOG.debug("Transaction signing failed");
            return new RpcMsg(null, RpcError.INTERNAL_ERROR, "Error in signing the transaction.");
        }
    }

    private RpcMsg processTxResponse(ApiTxResponse rsp) {
        switch (rsp.getType()) {
            case SUCCESS:
            case ALREADY_CACHED:
            case CACHED_POOLMAX:
            case CACHED_NONCE:
            case ALREADY_SEALED:
            case REPAID:
                return new RpcMsg(StringUtils.toJsonHex(rsp.getTxHash()));
            case INVALID_TX:
            case INVALID_TX_NRG_PRICE:
            case INVALID_FROM:
            case REPAYTX_LOWPRICE:
                return new RpcMsg(null, RpcError.INVALID_PARAMS, rsp.getMessage());
            case INVALID_ACCOUNT:
                return new RpcMsg(null, RpcError.NOT_ALLOWED, rsp.getMessage());

            case REPAYTX_POOL_EXCEPTION:
            case DROPPED:
            case EXCEPTION:
            default:
                return new RpcMsg(null, RpcError.EXECUTION_ERROR, rsp.getMessage());
        }
    }

    public RpcMsg eth_sendTransaction(Object _params) {
        JSONObject _tx;
        if (_params instanceof JSONArray) {
            _tx = ((JSONArray) _params).getJSONObject(0);
        } else if (_params instanceof JSONObject) {
            _tx = ((JSONObject) _params).getJSONObject("transaction");
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        ArgTxCall txParams = ArgTxCall.fromJSON(_tx, getRecommendedNrgPrice());

        ApiTxResponse response = sendTransaction(txParams);

        return processTxResponse(response);
    }

    public RpcMsg eth_sendRawTransaction(Object _params) {
        String _rawTx;
        if (_params instanceof JSONArray) {
            _rawTx = ((JSONArray) _params).get(0) + "";
        } else if (_params instanceof JSONObject) {
            _rawTx = ((JSONObject) _params).get("transaction") + "";
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        if (_rawTx.equals("null")) {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Null raw transaction provided.");
        }

        byte[] rawTransaction = ByteUtil.hexStringToBytes(_rawTx);

        ApiTxResponse response = sendTransaction(rawTransaction);

        return processTxResponse(response);
    }

    public RpcMsg eth_call(Object _params) {
        JSONObject _tx;
        Object _bnOrId;
        if (_params instanceof JSONArray) {
            _tx = ((JSONArray) _params).getJSONObject(0);
            _bnOrId = ((JSONArray) _params).opt(1);
        } else if (_params instanceof JSONObject) {
            _tx = ((JSONObject) _params).getJSONObject("transaction");
            _bnOrId = ((JSONObject) _params).opt("block");
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        ArgTxCall txParams = ArgTxCall.fromJSONforCall(_tx, getRecommendedNrgPrice());

        if (txParams == null) {
            return new RpcMsg(
                    null, RpcError.INVALID_PARAMS, "Invalid transaction parameter provided");
        }

        String bnOrId = "latest";
        if (!JSONObject.NULL.equals(_bnOrId)) {
            bnOrId = _bnOrId + "";
        }

        Long bn = parseBnOrId(bnOrId);
        if (bn == null) {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid block number.");
        }

        Block b = getBlockByBN(bn);

        AionAddress sender = txParams.getFrom();
        if (sender == null) {
            sender = AddressUtils.ZERO_ADDRESS;
        }

        AionTransaction tx =
                AionTransaction.createWithoutKey(
                        txParams.getNonce().toByteArray(),
                        sender,
                        txParams.getTo(),
                        txParams.getValue().toByteArray(),
                        txParams.getData(),
                        txParams.getNrg(),
                        txParams.getNrgPrice(),
                        TransactionTypes.DEFAULT);

        AionTxReceipt receipt = this.ac.callConstant(tx, b);

        return new RpcMsg(StringUtils.toJsonHex(receipt.getTransactionOutput()));
    }

    public RpcMsg eth_estimateGas(Object _params) {
        JSONObject _tx;
        if (_params instanceof JSONArray) {
            _tx = ((JSONArray) _params).getJSONObject(0);
        } else if (_params instanceof JSONObject) {
            _tx = ((JSONObject) _params).getJSONObject("transaction");
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        ArgTxCall txParams = ArgTxCall.fromJSONforCall(_tx, getRecommendedNrgPrice());

        NumericalValue estimate = new NumericalValue(estimateNrg(txParams));

        return new RpcMsg(estimate.toHexString());
    }

    public RpcMsg eth_getBlockByHash(Object _params) {
        String _hash;
        boolean _fullTx;
        try {
            if (_params instanceof JSONArray) {
                _hash = ((JSONArray) _params).get(0) + "";
                _fullTx = ((JSONArray) _params).optBoolean(1, false);
            } else if (_params instanceof JSONObject) {
                _hash = ((JSONObject) _params).get("block") + "";
                _fullTx = ((JSONObject) _params).optBoolean("fullTransaction", false);
            } else {
                throw new Exception("Invalid input object provided");
            }
        } catch (Exception e) {
            LOG.debug("Error processing json input arguments", e);
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        byte[] hash = ByteUtil.hexStringToBytes(_hash);

        Block block = this.ac.getBlockchain().getBlockByHash(hash);
        if (block == null) {
            LOG.debug("<get-block hash={} err=not-found>", _hash);
            return new RpcMsg(JSONObject.NULL); // json rpc spec: 'or null when no block was found'
        }

        byte[] mainchainHash =
                this.ac.getAionHub().getBlockStore().getBlockHashByNumber(block.getNumber());
        if (mainchainHash == null) {
            LOG.debug("<get-block hash={} err=not-found>", _hash);
            return new RpcMsg(JSONObject.NULL); // json rpc spec: 'or null when no block was found'
        }

        if (!Arrays.equals(block.getHash(), mainchainHash)) {
            LOG.debug("<rpc-server not mainchain>", _hash);
            return new RpcMsg(JSONObject.NULL);
        }

        BigInteger totalDiff = this.ac.getAionHub().getBlockStore().getTotalDifficultyForHash(hash);
        return new RpcMsg(Blk.AionBlockToJson(block, totalDiff, _fullTx));
    }

    public RpcMsg eth_getBlockByNumber(Object _params) {
        String _bnOrId;
        boolean _fullTx;
        try {
            if (_params instanceof JSONArray) {
                _bnOrId = ((JSONArray) _params).get(0) + "";
                _fullTx = ((JSONArray) _params).optBoolean(1, false);
            } else if (_params instanceof JSONObject) {
                _bnOrId = ((JSONObject) _params).get("block") + "";
                _fullTx = ((JSONObject) _params).optBoolean("fullTransaction", false);
            } else {
                throw new Exception("Invalid input object provided");
            }
        } catch (Exception e) {
            LOG.debug("Error processing json input arguments", e);
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        Long bn = this.parseBnOrId(_bnOrId);

        if (bn == null) {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid block number.");
        }

        Block nb = getBlockByBN(bn);

        if (nb == null) {
            LOG.debug("<get-block bn={} err=not-found>", bn);
            return new RpcMsg(JSONObject.NULL); // json rpc spec: 'or null when no block was found'
        }

        // add main chain block to cache (currently only used by ops_getTransactionReceipt_*
        // functions)
        blockCache.put(new ByteArrayWrapper(nb.getHash()), nb);
        BigInteger totalDiff =
                this.ac.getAionHub().getBlockStore().getTotalDifficultyForHash(nb.getHash());
        return new RpcMsg(Blk.AionBlockToJson(nb, totalDiff, _fullTx));
    }

    public RpcMsg eth_getTransactionByHash(Object _params) {
        String _hash;
        try {
            if (_params instanceof JSONArray) {
                _hash = ((JSONArray) _params).get(0) + "";
            } else if (_params instanceof JSONObject) {
                _hash = ((JSONObject) _params).get("transactionHash") + "";
            } else {
                throw new Exception("Invalid input object provided");
            }
        } catch (Exception e) {
            LOG.debug("Error processing json input arguments", e);
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        byte[] txHash = ByteUtil.hexStringToBytes(_hash);
        if (_hash.equals("null") || txHash == null) {
            return null;
        }

        AionTxInfo txInfo = this.ac.getAionHub().getBlockchain().getTransactionInfo(txHash);
        if (txInfo == null) {
            return new RpcMsg(
                    JSONObject.NULL); // json rpc spec: 'or null when no transaction was found'
        }

        Block b = this.ac.getBlockchain().getBlockByHash(txInfo.getBlockHash());
        if (b == null) {
            return null; // this is actually an internal error
        }

        return new RpcMsg(Tx.InfoToJSON(txInfo, b));
    }

    public RpcMsg eth_getInternalTransactionsByHash(Object _params) {
        String _hash;
        try {
            if (_params instanceof JSONArray) {
                _hash = ((JSONArray) _params).get(0) + "";
            } else if (_params instanceof JSONObject) {
                _hash = ((JSONObject) _params).get("transactionHash") + "";
            } else {
                throw new Exception("Invalid input object provided");
            }
        } catch (Exception e) {
            LOG.debug("Error processing json input arguments", e);
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        byte[] txHash = ByteUtil.hexStringToBytes(_hash);
        if (_hash.equals("null") || txHash == null) {
            return null;
        }

        AionTxInfo txInfo = this.ac.getAionHub().getBlockchain().getTransactionInfo(txHash);
        if (txInfo == null) {
            return new RpcMsg(JSONObject.NULL); // json rpc spec: 'or null when no transaction was found'
        }

        Block b = this.ac.getBlockchain().getBlockByHash(txInfo.getBlockHash());
        if (b == null) {
            return null; // this is actually an internal error
        }

        return new RpcMsg(Tx.internalTxsToJSON(txInfo.getInternalTransactions(), txHash, txInfo.isCreatedWithInternalTransactions()));
    }

    public RpcMsg eth_getTransactionByBlockHashAndIndex(Object _params) {
        String _hash;
        String _index;
        if (_params instanceof JSONArray) {
            _hash = ((JSONArray) _params).get(0) + "";
            _index = ((JSONArray) _params).get(1) + "";
        } else if (_params instanceof JSONObject) {
            _hash = ((JSONObject) _params).get("blockHash") + "";
            _index = ((JSONObject) _params).get("index") + "";
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        byte[] hash = ByteUtil.hexStringToBytes(_hash);
        if (_hash.equals("null") || hash == null) {
            return null;
        }

        Block b = this.ac.getBlockchain().getBlockByHash(hash);
        if (b == null) {
            return new RpcMsg(
                    JSONObject.NULL); // json rpc spec: 'or null when no transaction was found'
        }

        List<AionTransaction> txs = b.getTransactionsList();

        int idx = Integer.decode(_index);
        if (idx >= txs.size()) {
            return new RpcMsg(
                    JSONObject.NULL); // json rpc spec: 'or null when no transaction was found'
        }

        return new RpcMsg(Tx.AionTransactionToJSON(txs.get(idx), b, idx));
    }

    public RpcMsg eth_getTransactionByBlockNumberAndIndex(Object _params) {
        String _bnOrId;
        String _index;
        if (_params instanceof JSONArray) {
            _bnOrId = ((JSONArray) _params).get(0) + "";
            _index = ((JSONArray) _params).get(1) + "";
        } else if (_params instanceof JSONObject) {
            _bnOrId = ((JSONObject) _params).get("block") + "";
            _index = ((JSONObject) _params).get("index") + "";
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        Long bn = parseBnOrId(_bnOrId);
        if (bn == null) {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid block number.");
        }

        Block b = this.getBlockByBN(bn);

        if (b == null) {
            return new RpcMsg(
                    JSONObject.NULL); // json rpc spec: 'or null when no transaction was found'
        }

        List<AionTransaction> txs = b.getTransactionsList();

        int idx = Integer.decode(_index);
        if (idx >= txs.size()) {
            return new RpcMsg(
                    JSONObject.NULL); // json rpc spec: 'or null when no transaction was found'
        }

        return new RpcMsg(Tx.AionTransactionToJSON(txs.get(idx), b, idx));
    }

    public RpcMsg eth_getTransactionReceipt(Object _params) {
        String _hash;
        if (_params instanceof JSONArray) {
            _hash = ((JSONArray) _params).get(0) + "";
        } else if (_params instanceof JSONObject) {
            _hash = ((JSONObject) _params).get("hash") + "";
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        byte[] txHash = StringUtils.StringHexToByteArray(_hash);
        TxRecpt r = getTransactionReceipt(txHash);

        // commenting this out because of lack support for old web3 client that we are using
        // TODO: re-enable this when we upgrade our web3 client
        /*
        // if we can't find the receipt on the mainchain, try looking for it in pending receipts cache
        /*
        if (r == null) {
            AionTxReceipt pendingReceipt = pendingReceipts.get(new ByteArrayWrapper(txHash));
            r = new TxRecpt(pendingReceipt, null, null, null, true);
        }
        */

        if (r == null) {
            return new RpcMsg(
                    JSONObject.NULL); // json rpc spec: 'or null when no receipt was found'
        }

        return new RpcMsg(r.toJson());
    }

    /* -------------------------------------------------------------------------
     * compiler
     */

    public RpcMsg eth_getCompilers() {
        return new RpcMsg(new JSONArray(this.compilers));
    }

    public RpcMsg eth_compileSolidity(Object _params) {
        String _contract;
        if (_params instanceof JSONArray) {
            _contract = ((JSONArray) _params).get(0) + "";
        } else if (_params instanceof JSONObject) {
            _contract = ((JSONObject) _params).get("contract") + "";
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        @SuppressWarnings("unchecked")
        Map<String, CompiledContr> compiled = contract_compileSolidity(_contract);
        JSONObject obj = new JSONObject();
        for (String key : compiled.keySet()) {
            CompiledContr cc = compiled.get(key);
            obj.put(key, cc.toJSON());
        }
        return new RpcMsg(obj);
    }

    public RpcMsg eth_compileSolidityZip(Object _params) {
        String _zipfile, _entrypoint;
        if (_params instanceof JSONArray) {
            _zipfile = ((JSONArray) _params).get(0) + "";
            _entrypoint = ((JSONArray) _params).get(1) + "";
        } else if (_params instanceof JSONObject) {
            _zipfile = ((JSONObject) _params).get("zipfile") + "";
            _entrypoint = ((JSONObject) _params).get("entrypoint") + "";
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        byte[] _zippedBytes = Base64.getDecoder().decode(_zipfile);

        @SuppressWarnings("unchecked")
        Map<String, CompiledContr> compiled =
                contract_compileSolidityZip(_zippedBytes, _entrypoint);
        JSONObject obj = new JSONObject();
        for (String key : compiled.keySet()) {
            CompiledContr cc = compiled.get(key);
            obj.put(key, cc.toJSON());
        }
        return new RpcMsg(obj);
    }

    /* -------------------------------------------------------------------------
     * filters
     */

    /* Web3 Filters Support
     *
     * NOTE: newFilter behaviour is ill-defined in the JSON-rpc spec for the following scenarios:
     * (an explanation of how we resolved these ambiguities follows immediately after)
     *
     * newFilter is used to subscribe for filter on transaction logs for transactions with provided address and topics
     *
     * role of fromBlock, toBlock fields within context of newFilter, newBlockFilter, newPendingTransactionFilter
     * (they seem only more pertinent for getLogs)
     * how we resolve it: populate historical data (best-effort) in the filter response before "installing the filter"
     * onus on the user to flush the filter of the historical data, before depending on it for up-to-date values.
     * apart from loading historical data, fromBlock & toBlock are ignored when loading events on filter queue
     */
    private FltrLg createFilter(ArgFltr rf) {
        FltrLg filter = new FltrLg();
        filter.setTopics(rf.topics);
        filter.setContractAddress(rf.address);

        Long bnFrom = parseBnOrId(rf.fromBlock);
        Long bnTo = parseBnOrId(rf.toBlock);

        if (bnFrom == null || bnTo == null) {
            LOG.debug("jsonrpc - eth_newFilter(): from, to block parse failed");
            return null;
        }

        if (bnTo != BEST_PENDING_BLOCK && bnFrom > bnTo) {
            LOG.debug("jsonrpc - eth_newFilter(): from block is after to block");
            return null;
        }

        Block fromBlock = this.getBlockByBN(bnFrom);
        Block toBlock = this.getBlockByBN(bnTo);

        if (fromBlock != null) {
            // need to add historical data
            // this is our own policy: what to do in this case is not defined in the spec
            //
            // policy: add data from earliest to latest, until we can't fill the queue anymore
            //
            // caveat: filling up the events-queue with historical data will cause the following
            // issue:
            // the user will miss all events generated between the first poll and filter
            // installation.

            toBlock = toBlock == null ? getBestBlock() : toBlock;
            for (long i = fromBlock.getNumber(); i <= toBlock.getNumber(); i++) {
                if (filter.isFull()) {
                    break;
                }
                filter.onBlock(
                        this.ac.getBlockchain().getBlockByNumber(i),
                        this.ac.getAionHub().getBlockchain());
            }
        }

        return filter;
    }

    public RpcMsg eth_newFilter(Object _params) {
        if (!isFilterEnabled) {
            return new RpcMsg(null, RpcError.NOT_ALLOWED, "Filters over rpc disabled.");
        }

        JSONObject _filterObj;
        if (_params instanceof JSONArray) {
            _filterObj = ((JSONArray) _params).getJSONObject(0);
        } else if (_params instanceof JSONObject) {
            _filterObj = ((JSONObject) _params).getJSONObject("filter");
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        ArgFltr rf = ArgFltr.fromJSON(_filterObj);
        if (rf == null) {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid filter object provided.");
        }

        FltrLg filter = createFilter(rf);
        if (filter == null) {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid filter object provided.");
        }

        // "install" the filter after populating historical data;
        // rationale: until the user gets the id back, the user should not expect the filter to be
        // "installed" anyway.
        long id = fltrIndex.getAndIncrement();
        installedFilters.put(id, filter);

        return new RpcMsg(StringUtils.toJsonHex(id));
    }

    public RpcMsg eth_newBlockFilter() {
        if (!isFilterEnabled) {
            return new RpcMsg(null, RpcError.NOT_ALLOWED, "Filters over rpc disabled.");
        }

        long id = fltrIndex.getAndIncrement();
        installedFilters.put(id, new FltrBlk());
        return new RpcMsg(StringUtils.toJsonHex(id));
    }

    public RpcMsg eth_newPendingTransactionFilter() {
        if (!isFilterEnabled) {
            return new RpcMsg(null, RpcError.NOT_ALLOWED, "Filters over rpc disabled.");
        }

        long id = fltrIndex.getAndIncrement();
        installedFilters.put(id, new FltrTx());
        return new RpcMsg(StringUtils.toJsonHex(id));
    }

    public RpcMsg eth_uninstallFilter(Object _params) {
        if (!isFilterEnabled) {
            return new RpcMsg(null, RpcError.NOT_ALLOWED, "Filters over rpc disabled.");
        }

        String _id;
        if (_params instanceof JSONArray) {
            _id = ((JSONArray) _params).get(0) + "";
        } else if (_params instanceof JSONObject) {
            _id = ((JSONObject) _params).get("id") + "";
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        return new RpcMsg(
                installedFilters.remove(StringUtils.StringHexToBigInteger(_id).longValue())
                        != null);
    }

    private JSONArray buildFilterResponse(Fltr filter) {
        Object[] events = filter.poll();
        JSONArray response = new JSONArray();
        for (Object event : events) {
            if (event instanceof Evt) {
                // put the Object we get out of the Evt object in here
                response.put(((Evt) event).toJSON());
            }
        }
        return response;
    }

    public RpcMsg eth_getFilterChanges(Object _params) {
        if (!isFilterEnabled) {
            return new RpcMsg(null, RpcError.NOT_ALLOWED, "Filters over rpc disabled.");
        }

        String _id;
        if (_params instanceof JSONArray) {
            _id = ((JSONArray) _params).get(0) + "";
        } else if (_params instanceof JSONObject) {
            _id = ((JSONObject) _params).get("id") + "";
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        long id = StringUtils.StringHexToBigInteger(_id).longValue();
        Fltr filter = installedFilters.get(id);

        if (filter == null) {
            return new RpcMsg(null, RpcError.EXECUTION_ERROR, "Filter not found.");
        }

        return new RpcMsg(buildFilterResponse(filter));
    }

    public RpcMsg eth_getLogs(Object _params) {
        JSONObject _filterObj;
        if (_params instanceof JSONArray) {
            _filterObj = ((JSONArray) _params).getJSONObject(0);
        } else if (_params instanceof JSONObject) {
            _filterObj = ((JSONObject) _params).getJSONObject("filter");
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        ArgFltr rf = ArgFltr.fromJSON(_filterObj);
        if (rf == null) {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid filter object provided.");
        }

        FltrLg filter = createFilter(rf);
        if (filter == null) {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid block ids provided.");
        }

        return new RpcMsg(buildFilterResponse(filter));
    }

    /* -------------------------------------------------------------------------
     * personal
     */

    public RpcMsg personal_unlockAccount(Object _params) {
        String _account;
        String _password;
        Object _duration;
        if (_params instanceof JSONArray) {
            _account = ((JSONArray) _params).get(0) + "";
            _password = ((JSONArray) _params).get(1) + "";
            _duration = ((JSONArray) _params).opt(2);
        } else if (_params instanceof JSONObject) {
            _account = ((JSONObject) _params).get("address") + "";
            _password = ((JSONObject) _params).get("password") + "";
            _duration = ((JSONObject) _params).opt("duration");
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        int duration = 300;
        if (!JSONObject.NULL.equals(_duration)) {
            duration = new BigInteger(_duration + "").intValueExact();
        }

        return new RpcMsg(unlockAccount(_account, _password, duration));
    }

    public RpcMsg personal_lockAccount(Object _params) {
        String _account;
        String _password;
        if (_params instanceof JSONArray) {
            _account = ((JSONArray) _params).get(0) + "";
            _password = ((JSONArray) _params).get(1) + "";
        } else if (_params instanceof JSONObject) {
            _account = ((JSONObject) _params).get("address") + "";
            _password = ((JSONObject) _params).get("password") + "";
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        return new RpcMsg(lockAccount(AddressUtils.wrapAddress(_account), _password));
    }

    public RpcMsg personal_newAccount(Object _params) {
        String _password;
        if (_params instanceof JSONArray) {
            _password = ((JSONArray) _params).get(0) + "";
        } else if (_params instanceof JSONObject) {
            _password = ((JSONObject) _params).get("password") + "";
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        String address = Keystore.create(_password);

        return new RpcMsg(StringUtils.toJsonHex(address));
    }

    /* -------------------------------------------------------------------------
     * debug
     */

    public RpcMsg debug_getBlocksByNumber(Object _params) {
        String _bnOrId;
        boolean _fullTx;
        if (_params instanceof JSONArray) {
            _bnOrId = ((JSONArray) _params).get(0) + "";
            _fullTx = ((JSONArray) _params).optBoolean(1, false);
        } else if (_params instanceof JSONObject) {
            _bnOrId = ((JSONObject) _params).get("block") + "";
            _fullTx = ((JSONObject) _params).optBoolean("fullTransaction", false);
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        Long bn = parseBnOrId(_bnOrId);

        if (bn == null || bn < 0) {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid block number.");
        }

        List<Map.Entry<Block, Map.Entry<BigInteger, Boolean>>> blocks =
                ((AionBlockStore) this.ac.getAionHub().getBlockchain().getBlockStore())
                        .getBlocksByNumber(bn);
        if (blocks == null) {
            return new RpcMsg(null, RpcError.EXECUTION_ERROR, "Blocks requested not found.");
        }

        JSONArray response = new JSONArray();
        for (Map.Entry<Block, Map.Entry<BigInteger, Boolean>> block : blocks) {
            JSONObject b =
                    (JSONObject)
                            Blk.AionBlockToJson(block.getKey(), block.getValue().getKey(), _fullTx);
            b.put("mainchain", block.getValue().getValue());
            response.put(b);
        }
        return new RpcMsg(response);
    }

    /* -------------------------------------------------------------------------
     * private debugging APIs
     * Reasoning for not adding this to conventional web3 calls is so
     * we can freely change the responses without breaking compatibility
     */
    public RpcMsg priv_peers() {
        Map<Integer, NodeWrapper> activeNodes = this.ac.getAionHub().getActiveNodes();

        JSONArray peerList = new JSONArray();

        for (NodeWrapper node : activeNodes.values()) {
            JSONObject n = new JSONObject();
            n.put("idShort", node.getIdShort());
            n.put("id", new String(node.getId()));
            n.put("idHash", node.getIdHash());
            n.put("version", node.getBinaryVersion());
            n.put("blockNumber", node.getBestBlockNumber());
            n.put("totalDifficulty", node.getTotalDifficulty());

            JSONObject network = new JSONObject();
            network.put("remoteAddress", node.getIpStr() + ":" + node.getPort());
            n.put("network", network);
            n.put("latestTimestamp", node.getTimestamp());

            // generate a date corresponding to UTC date time (not local)
            String utcTimestampDate =
                    Instant.ofEpochMilli(node.getTimestamp()).atOffset(ZoneOffset.UTC).toString();
            n.put("latestTimestampUTC", utcTimestampDate);
            n.put("version", node.getBinaryVersion());

            peerList.put(n);
        }
        return new RpcMsg(peerList);
    }

    public RpcMsg priv_p2pConfig() {
        CfgNetP2p p2p = CfgAion.inst().getNet().getP2p();

        JSONObject obj = new JSONObject();
        obj.put("localBinding", p2p.getIp() + ":" + p2p.getPort());
        return new RpcMsg(obj);
    }

    // default block for pending transactions
    private static final AionBlock defaultBlock = AionBlock.newEmptyBlock();

    public RpcMsg priv_getPendingTransactions(Object _params) {
        boolean fullTx = ((JSONArray) _params).optBoolean(0, false);
        List<AionTransaction> transactions = this.ac.getPendingStateTransactions();

        JSONArray arr = new JSONArray();
        for (int i = 0; i < transactions.size(); i++) {
            if (fullTx) {
                arr.put(Tx.AionTransactionToJSON(transactions.get(i), defaultBlock, i));
            } else {
                arr.put(ByteUtil.toHexString(transactions.get(i).getTransactionHash()));
            }
        }
        return new RpcMsg(arr);
    }

    public RpcMsg priv_getPendingSize() {
        return new RpcMsg(this.ac.getPendingStateTransactions().size());
    }

    public RpcMsg priv_dumpTransaction(Object _params) {
        String transactionHash;
        if (_params instanceof JSONArray) {
            transactionHash = ((JSONArray) _params).get(0) + "";
        } else if (_params instanceof JSONObject) {
            transactionHash = ((JSONObject) _params).get("hash") + "";
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        byte[] hash = ByteUtil.hexStringToBytes(transactionHash);
        if (hash == null) {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid transaction hash");
        }

        // begin output processing
        AionTxInfo transaction = this.ac.getAionHub().getBlockchain().getTransactionInfo(hash);

        if (transaction == null) {
            return new RpcMsg(JSONObject.NULL);
        }

        JSONObject tx =
                Tx.InfoToJSON(
                        transaction,
                        this.ac.getBlockchain().getBlockByHash(transaction.getBlockHash()));
        String raw = ByteUtil.toHexString(transaction.getReceipt().getTransaction().getEncoded());

        JSONObject obj = new JSONObject();
        obj.put("transaction", tx);
        obj.put("raw", raw);
        return new RpcMsg(obj);
    }

    public RpcMsg priv_dumpBlockByHash(Object _params) {
        String hashString;
        if (_params instanceof JSONArray) {
            hashString = ((JSONArray) _params).get(0) + "";
        } else if (_params instanceof JSONObject) {
            hashString = ((JSONObject) _params).get("hash") + "";
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        byte[] hash = ByteUtil.hexStringToBytes(hashString);
        if (hash == null) {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid block hash");
        }

        Block block = this.ac.getBlockchain().getBlockByHash(hash);

        if (block == null) {
            return new RpcMsg(JSONObject.NULL);
        }

        BigInteger totalDiff = this.ac.getBlockchain().getTotalDifficultyByHash(new Hash256(hash));
        return new RpcMsg(dumpBlock(block, totalDiff, false));
    }

    public RpcMsg priv_dumpBlockByNumber(Object _params) {
        String numberString;
        if (_params instanceof JSONArray) {
            numberString = ((JSONArray) _params).get(0) + "";
        } else if (_params instanceof JSONObject) {
            numberString = ((JSONObject) _params).get("number") + "";
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        // TODO: parse hex
        long number;
        try {
            number = Long.parseLong(numberString);
        } catch (NumberFormatException e) {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Unable to decode input number");
        }
        Block block = this.ac.getBlockchain().getBlockByNumber(number);

        if (block == null) {
            return new RpcMsg(JSONObject.NULL);
        }

        BigInteger totalDiff =
                this.ac.getBlockchain().getTotalDifficultyByHash(new Hash256(block.getHash()));
        return new RpcMsg(dumpBlock(block, totalDiff, false));
    }

    private static JSONObject dumpBlock(Block block, BigInteger totalDiff, boolean full) {
        JSONObject obj = new JSONObject();
        obj.put("block", Blk.AionBlockToJson(block, totalDiff, full));
        obj.put("raw", ByteUtil.toHexString(block.getEncoded()));
        return obj;
    }

    /**
     * Very short blurb generated about our most important stats, intended for quick digestion and
     * monitoring tool usage
     */
    // TODO
    public RpcMsg priv_shortStats() {
        Block block = this.ac.getBlockchain().getBestBlock();
        Map<Integer, NodeWrapper> peer = this.ac.getAionHub().getActiveNodes();

        // this could be optimized (cached)
        NodeWrapper maxPeer = null;
        for (NodeWrapper p : peer.values()) {
            if (maxPeer == null) {
                maxPeer = p;
                continue;
            }

            if (p.getTotalDifficulty().compareTo(maxPeer.getTotalDifficulty()) > 0) {
                maxPeer = p;
            }
        }

        // basic local configuration
        CfgAion config = CfgAion.inst();

        JSONObject obj = new JSONObject();
        obj.put("id", config.getId());
        obj.put("genesisHash", ByteUtil.toHexString(config.getGenesis().getHash()));
        obj.put("version", Version.KERNEL_VERSION);
        obj.put("bootBlock", this.ac.getAionHub().getStartingBlock().getNumber());

        long time = System.currentTimeMillis();
        obj.put("timestamp", time);
        obj.put("timestampUTC", Instant.ofEpochMilli(time).atOffset(ZoneOffset.UTC).toString());

        // base.blockchain
        JSONObject blockchain = new JSONObject();
        blockchain.put("bestBlockhash", ByteUtil.toHexString(block.getHash()));
        blockchain.put("bestNumber", block.getNumber());
        blockchain.put(
                "totalDifficulty",
                this.ac.getBlockchain().getTotalDifficultyByHash(new Hash256(block.getHash())));
        // end
        obj.put("local", blockchain);

        // base.network
        JSONObject network = new JSONObject();
        // remote
        if (maxPeer != null) {
            // base.network.best
            JSONObject remote = new JSONObject();
            remote.put("id", new String(maxPeer.getId()));
            remote.put("totalDifficulty", maxPeer.getTotalDifficulty());
            remote.put("bestNumber", maxPeer.getBestBlockNumber());
            remote.put("version", maxPeer.getBinaryVersion());
            remote.put("timestamp", maxPeer.getTimestamp());
            remote.put(
                    "timestampUTC",
                    Instant.ofEpochMilli(maxPeer.getTimestamp())
                            .atOffset(ZoneOffset.UTC)
                            .toString());
            // end
            network.put("best", remote);
        }

        // end
        network.put("peerCount", peer.size());
        obj.put("network", network);

        return new RpcMsg(obj);
    }

    /**
     * This may seem similar to a superset of peers, with the difference being that this should only
     * contain a subset of peers we are actively syncing from
     */
    public RpcMsg priv_syncPeers() {
        // contract here is we do NOT modify the peerStates in any way
        Map<Integer, PeerState> peerStates = this.ac.getAionHub().getSyncMgr().getPeerStates();

        // also retrieve nodes from p2p to see if we can piece together a full state
        Map<Integer, NodeWrapper> nodeState = this.ac.getAionHub().getActiveNodes();

        JSONArray array = new JSONArray();
        for (Map.Entry<Integer, PeerState> peerState : peerStates.entrySet()) {
            // begin []
            JSONObject peerObj = new JSONObject();
            NodeWrapper node;
            if ((node = nodeState.get(peerState.getKey())) != null) {
                // base[].node
                JSONObject nodeObj = new JSONObject();
                nodeObj.put("id", new String(node.getId()));
                nodeObj.put("totalDifficulty", node.getTotalDifficulty());
                nodeObj.put("bestNumber", node.getBestBlockNumber());
                nodeObj.put("version", node.getBinaryVersion());
                nodeObj.put("timestamp", node.getTimestamp());
                nodeObj.put(
                        "timestampUTC",
                        Instant.ofEpochMilli(node.getTimestamp())
                                .atOffset(ZoneOffset.UTC)
                                .toString());

                // end
                peerObj.put("node", nodeObj);
            }

            PeerState ps = peerState.getValue();
            peerObj.put("idHash", peerState.getKey());
            peerObj.put("lastRequestTimestamp", ps.getLastHeaderRequest());
            peerObj.put(
                    "lastRequestTimestampUTC",
                    Instant.ofEpochMilli(ps.getLastHeaderRequest())
                            .atOffset(ZoneOffset.UTC)
                            .toString());
            peerObj.put("mode", ps.getMode().toString());
            peerObj.put("base", ps.getBase());

            // end
            array.put(peerObj);
        }
        return new RpcMsg(array);
    }

    public RpcMsg priv_config() {
        JSONObject obj = new JSONObject();

        CfgAion config = CfgAion.inst();

        obj.put("id", config.getId());
        obj.put("basePath", config.getBasePath());

        obj.put("net", configNet());
        obj.put("consensus", configConsensus());
        obj.put("sync", configSync());
        obj.put("api", configApi());
        obj.put("db", configDb());
        obj.put("tx", configTx());

        return new RpcMsg(obj);
    }

    // TODO: we can refactor these in the future to be in
    // their respective classes, for now put the toJson here

    private static JSONObject configNet() {
        CfgNet config = CfgAion.inst().getNet();
        JSONObject obj = new JSONObject();

        // begin base.net.p2p
        CfgNetP2p configP2p = config.getP2p();
        JSONObject p2p = new JSONObject();
        p2p.put("ip", configP2p.getIp());
        p2p.put("port", configP2p.getPort());
        p2p.put("discover", configP2p.getDiscover());
        p2p.put("errorTolerance", configP2p.getErrorTolerance());
        p2p.put("maxActiveNodes", configP2p.getMaxActiveNodes());
        p2p.put("maxTempNodes", configP2p.getMaxTempNodes());
        p2p.put("clusterNodeMode", configP2p.inClusterNodeMode());
        p2p.put("syncOnlyMode", configP2p.inSyncOnlyMode());

        // end
        obj.put("p2p", p2p);

        // begin base.net.nodes[]
        JSONArray nodeArray = new JSONArray();
        for (String n : config.getNodes()) {
            nodeArray.put(n);
        }

        // end
        obj.put("nodes", nodeArray);
        // begin base
        obj.put("id", config.getId());

        // end
        return obj;
    }

    private static JSONObject configConsensus() {
        CfgConsensusPow config = CfgAion.inst().getConsensus();
        JSONObject obj = new JSONObject();
        obj.put("mining", config.getMining());
        obj.put("minerAddress", config.getMinerAddress());
        obj.put("threads", config.getCpuMineThreads());
        obj.put("extraData", config.getExtraData());
        obj.put("isSeed", config.isSeed());

        // base.consensus.energyStrategy
        CfgEnergyStrategy nrg = config.getEnergyStrategy();
        JSONObject nrgObj = new JSONObject();
        nrgObj.put("strategy", nrg.getStrategy());
        nrgObj.put("target", nrg.getTarget());
        nrgObj.put("upper", nrg.getUpperBound());
        nrgObj.put("lower", nrg.getLowerBound());

        // end
        obj.put("energyStrategy", nrgObj);
        return obj;
    }

    private static JSONObject configSync() {
        CfgSync config = CfgAion.inst().getSync();
        JSONObject obj = new JSONObject();
        obj.put("showStatus", config.getShowStatus());
        obj.put("blocksQueueMax", config.getBlocksQueueMax());
        return obj;
    }

    private static JSONObject configApi() {
        CfgApi config = CfgAion.inst().getApi();

        JSONObject obj = new JSONObject();

        // base.api.rpc
        CfgApiRpc rpcConfig = config.getRpc();
        CfgSsl sslConfig = rpcConfig.getSsl();
        JSONObject rpc = new JSONObject();
        rpc.put("ip", rpcConfig.getIp());
        rpc.put("port", rpcConfig.getPort());
        rpc.put("corsEnabled", rpcConfig.isCorsEnabled());
        rpc.put("active", rpcConfig.isActive());
        rpc.put("maxThread", rpcConfig.getWorkerThreads());
        rpc.put("sslEnabled", sslConfig.getEnabled());
        rpc.put("sslCert", sslConfig.getCert());
        rpc.put("sslPass", sslConfig.getPass());

        // end
        obj.put("rpc", rpc);

        // base.api.zmq
        CfgApiZmq zmqConfig = config.getZmq();
        JSONObject zmq = new JSONObject();

        zmq.put("ip", zmqConfig.getIp());
        zmq.put("port", zmqConfig.getPort());
        zmq.put("active", zmqConfig.getActive());

        // end
        obj.put("zmq", zmq);

        // base.api.nrg
        CfgApiNrg nrgConfig = config.getNrg();
        JSONObject nrg = new JSONObject();

        nrg.put("defaultPrice", nrgConfig.getNrgPriceDefault());
        nrg.put("maxPrice", nrgConfig.getNrgPriceMax());

        // end
        obj.put("nrg", nrg);
        return obj;
    }

    // this is temporarily disabled until DB configuration changes come in
    private static JSONObject configDb() {
        return new JSONObject();
    }

    private static JSONObject configTx() {
        CfgTx config = CfgAion.inst().getTx();
        JSONObject obj = new JSONObject();
        obj.put("cacheMax", config.getCacheMax());
        obj.put("poolBackup", config.getPoolBackup());
        obj.put("buffer", config.getBuffer());
        obj.put("poolDump", config.getPoolDump());
        return obj;
    }

    /* -------------------------------------------------------------------------
     * operational api
     */

    // always gets the latest account state
    public RpcMsg ops_getAccountState(Object _params) {
        String _address;
        if (_params instanceof JSONArray) {
            _address = ((JSONArray) _params).get(0) + "";
        } else if (_params instanceof JSONObject) {
            _address = ((JSONObject) _params).get("address") + "";
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        AionAddress address;

        try {
            address = AddressUtils.wrapAddress(_address);
        } catch (Exception e) {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid address provided.");
        }

        long latestBlkNum = this.getBestBlock().getNumber();
        AccountState accountState =
                ((AionRepositoryImpl) this.ac.getRepository()).getAccountState(address);

        BigInteger nonce = BigInteger.ZERO;
        BigInteger balance = BigInteger.ZERO;

        if (accountState != null) {
            nonce = accountState.getNonce();
            balance = accountState.getBalance();
        }

        JSONObject response = new JSONObject();
        response.put("address", address.toString());
        response.put("blockNumber", latestBlkNum);
        response.put("balance", StringUtils.toJsonHex(balance));
        response.put("nonce", StringUtils.toJsonHex(nonce));

        return new RpcMsg(response);
    }

    // always gets the latest 20 blocks and transactions
    private class ChainHeadView {

        LinkedList<byte[]> hashQueue; // more precisely a dequeue
        Map<byte[], JSONObject> blkList;
        Map<byte[], Block> blkObjList;
        Map<byte[], List<AionTransaction>> txnList;

        private JSONObject response;

        private int qSize;

        public ChainHeadView(ChainHeadView cv) {
            hashQueue = new LinkedList<>(cv.hashQueue);
            blkList = new HashMap<>(cv.blkList);
            blkObjList = new HashMap<>(cv.blkObjList);
            txnList = new HashMap<>(cv.txnList);
            response = new JSONObject(cv.response, JSONObject.getNames(cv.response));
            qSize = cv.qSize;
        }

        public ChainHeadView(int _qSize) {
            hashQueue = new LinkedList<>();
            blkList = new HashMap<>();
            blkObjList = new HashMap<>();
            txnList = new HashMap<>();
            response = new JSONObject();
            qSize = _qSize;
        }

        private JSONObject getJson(Block _b) {
            BigInteger totalDiff =
                    ac.getAionHub().getBlockStore().getTotalDifficultyForHash(_b.getHash());
            return Blk.AionBlockOnlyToJson(_b, totalDiff);
        }

        private JSONObject buildResponse() {
            JSONArray blks = new JSONArray();
            JSONArray txns = new JSONArray();

            // return qSize number of blocks and transactions as json
            for (int i = 0; i < hashQueue.size(); i++) {
                byte[] hash = hashQueue.get(i);
                JSONObject blk = blkList.get(hash);
                if (i < hashQueue.size() - 1) {
                    Block blkThis = blkObjList.get(hash);
                    Block blkNext = blkObjList.get(hashQueue.get(i + 1));
                    blk.put("blockTime", blkThis.getTimestamp() - blkNext.getTimestamp());
                }
                blks.put(blk);
                List<AionTransaction> t = txnList.get(hash);

                for (int j = 0; (j < t.size() && txns.length() <= qSize); j++) {
                    txns.put(Tx.AionTransactionToJSON(t.get(j), blkObjList.get(hash), j));
                }
            }

            JSONObject metrics;
            try {
                metrics = computeMetrics();
            } catch (Exception e) {
                LOG.error("failed to compute metrics.", e);
                metrics = new JSONObject();
            }

            JSONObject o = new JSONObject();
            o.put("blocks", blks);
            o.put("transactions", txns);
            o.put("metrics", metrics);
            return o;
        }

        private JSONObject computeMetrics() {
            long blkTimeAccumulator = 0L;
            BigInteger lastDifficulty = null;
            BigInteger difficultyAccumulator = new BigInteger("0");
            BigInteger nrgConsumedAccumulator = new BigInteger("0");
            BigInteger nrgLimitAccumulator = new BigInteger("0");
            long txnCount = 0L;

            int count = blkObjList.size();
            Long lastBlkTimestamp = null;
            Block b = null;
            ListIterator li = hashQueue.listIterator(0);
            while (li.hasNext()) {
                byte[] hash = (byte[]) li.next();
                b = blkObjList.get(hash);

                if (lastBlkTimestamp != null) {
                    blkTimeAccumulator += (lastBlkTimestamp - b.getTimestamp());
                }
                lastBlkTimestamp = b.getTimestamp();

                difficultyAccumulator =
                        difficultyAccumulator.add(new BigInteger(b.getDifficulty()));
                lastDifficulty = new BigInteger(b.getDifficulty());

                nrgConsumedAccumulator =
                        nrgConsumedAccumulator.add(
                                new BigInteger(Long.toString(b.getNrgConsumed())));
                nrgLimitAccumulator =
                        nrgLimitAccumulator.add(new BigInteger(Long.toString(b.getNrgLimit())));
                txnCount += b.getTransactionsList().size();
            }

            BigInteger lastBlkReward =
                    ((AionBlockchainImpl) ac.getBlockchain())
                            .getChainConfiguration()
                            .getRewardsCalculator()
                            .calculateReward(b.getHeader().getNumber());

            double blkTime = 0;
            double hashRate = 0;
            double avgDifficulty = 0;
            double avgNrgConsumedPerBlock = 0;
            double avgNrgLimitPerBlock = 0;
            double txnPerSec = 0;

            if (count > 0 && blkTimeAccumulator > 0) {
                blkTime = blkTimeAccumulator / (double) count;
                hashRate = lastDifficulty.longValue() / blkTime;
                avgDifficulty = difficultyAccumulator.longValue() / (double) count;
                avgNrgConsumedPerBlock = nrgConsumedAccumulator.longValue() / (double) count;
                avgNrgLimitPerBlock = nrgLimitAccumulator.longValue() / (double) count;
                txnPerSec = txnCount / (double) blkTimeAccumulator;
            }

            long startBlock = 0;
            long endBlock = 0;
            long startTimestamp = 0;
            long endTimestamp = 0;
            long currentBlockchainHead = 0;

            if (hashQueue.size() > 0) {
                Block startBlockObj = blkObjList.get(hashQueue.peekLast());
                Block endBlockObj = blkObjList.get(hashQueue.peekFirst());

                startBlock = startBlockObj.getNumber();
                endBlock = endBlockObj.getNumber();
                startTimestamp = startBlockObj.getTimestamp();
                endTimestamp = endBlockObj.getTimestamp();
                currentBlockchainHead = endBlock;
            }

            JSONObject metrics = new JSONObject();
            metrics.put("averageDifficulty", avgDifficulty);
            metrics.put("averageBlockTime", blkTime);
            metrics.put("hashRate", hashRate);
            metrics.put("transactionPerSecond", txnPerSec);
            metrics.put("lastBlockReward", lastBlkReward);
            metrics.put("targetBlockTime", 10);
            metrics.put("blockWindow", OPS_RECENT_ENTITY_COUNT);

            metrics.put("startBlock", startBlock);
            metrics.put("endBlock", endBlock);
            metrics.put("startTimestamp", startTimestamp);
            metrics.put("endTimestamp", endTimestamp);
            metrics.put("currentBlockchainHead", currentBlockchainHead);

            metrics.put("averageNrgConsumedPerBlock", avgNrgConsumedPerBlock);
            metrics.put("averageNrgLimitPerBlock", avgNrgLimitPerBlock);

            return metrics;
        }

        ChainHeadView update() {
            // get the latest head
            Block blk = getBestBlock();

            if (Arrays.equals(hashQueue.peekFirst(), blk.getHash())) {
                return this; // nothing to do
            }

            // evict data as necessary
            LinkedList<Map.Entry<byte[], Map.Entry<Block, JSONObject>>> tempStack =
                    new LinkedList<>();
            tempStack.push(Map.entry(blk.getHash(), Map.entry(blk, getJson(blk))));
            int itr = 1; // deliberately 1, since we've already added the 0th element to the stack

            /*
            if (hashQueue.peekFirst() != null) {
                System.out.println("[" + 0 + "]: " + StringUtils.toJsonHex(hashQueue.peekFirst()) + " - " + blocks.get(hashQueue.peekFirst()).getNumber());
                System.out.println("----------------------------------------------------------");
                System.out.println("isParentHashMatch? " + FastByteComparisons.equal(hashQueue.peekFirst(), blk.getParentHash()));
                System.out.println("blk.getNumber() " + blk.getNumber());
            }
            System.out.println("blkNum: " + blk.getNumber() +
                    " parentHash: " + StringUtils.toJsonHex(blk.getParentHash()) +
                    " blkHash: " + StringUtils.toJsonHex(blk.getHash()));
            */

            while (!Arrays.equals(hashQueue.peekFirst(), blk.getParentHash())
                    && itr < qSize
                    && blk.getNumber() > 2) {

                blk = getBlockByHash(blk.getParentHash());
                tempStack.push(Map.entry(blk.getHash(), Map.entry(blk, getJson(blk))));
                itr++;
                /*
                System.out.println("blkNum: " + blk.getNumber() +
                        " parentHash: " + StringUtils.toJsonHex(blk.getParentHash()) +
                        " blkHash: " + StringUtils.toJsonHex(blk.getHash()));
                */
            }

            // evict out the right number of elements first
            for (int i = 0; i < tempStack.size(); i++) {
                byte[] tailHash = hashQueue.pollLast();
                if (tailHash != null) {
                    blkList.remove(tailHash);
                    blkObjList.remove(tailHash);
                    txnList.remove(tailHash);
                }
            }

            // empty out the stack into the queue
            while (!tempStack.isEmpty()) {
                // add to the queue
                Map.Entry<byte[], Map.Entry<Block, JSONObject>> element = tempStack.pop();
                byte[] hash = element.getKey();
                Block blkObj = element.getValue().getKey();
                JSONObject blkJson = element.getValue().getValue();
                List<AionTransaction> txnJson = blkObj.getTransactionsList();

                hashQueue.push(hash);
                blkList.put(hash, blkJson);
                blkObjList.put(hash, blkObj);
                txnList.put(hash, txnJson);
            }

            /*
            System.out.println("[" + 0 + "]: " + StringUtils.toJsonHex(hashQueue.peekFirst()) + " - " + blocks.get(hashQueue.peekFirst()).getNumber());
            System.out.println("----------------------------------------------------------");
            for (int i = hashQueue.size() - 1; i >= 0; i--) {
                System.out.println("[" + i + "]: " + StringUtils.toJsonHex(hashQueue.get(i)) + " - " + blocks.get(hashQueue.get(i)).getNumber());
            }
            */
            this.response = buildResponse();

            return this;
        }

        JSONObject getResponse() {
            return response;
        }

        long getViewBestBlock() {
            return blkObjList.get(hashQueue.peekFirst()).getNumber();
        }
    }

    private enum CachedResponseType {
        CHAIN_HEAD
    }

    public RpcMsg ops_getChainHeadView() {
        try {
            ChainHeadView v = CachedRecentEntities.get(CachedResponseType.CHAIN_HEAD.ordinal());
            return new RpcMsg(v.getResponse());
        } catch (Exception e) {
            LOG.error("<rpc-server - cannot get cached response for ops_getChainHeadView: ", e);
            return new RpcMsg(null, RpcError.EXECUTION_ERROR, "Cached response retrieve failed.");
        }
    }

    public RpcMsg ops_getChainHeadViewBestBlock() {
        try {
            ChainHeadView v = CachedRecentEntities.get(CachedResponseType.CHAIN_HEAD.ordinal());
            return new RpcMsg(v.getViewBestBlock());
        } catch (Exception e) {
            LOG.error("<rpc-server - cannot get cached response for ops_getChainHeadView: ", e);
            return new RpcMsg(null, RpcError.EXECUTION_ERROR, "Cached response retrieve failed.");
        }
    }

    // use a custom implementation to get a receipt with 2 db reads and a constant time op, as
    // opposed to
    // the getTransactionReceipt() in parent, which computes cumulativeNrg computatio for spec
    // compliance
    public RpcMsg ops_getTransaction(Object _params) {
        String _hash;
        if (_params instanceof JSONArray) {
            _hash = ((JSONArray) _params).get(0) + "";
        } else if (_params instanceof JSONObject) {
            _hash = ((JSONObject) _params).get("hash") + "";
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        byte[] txHash = StringUtils.StringHexToByteArray(_hash);

        if (txHash == null) {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        AionTxInfo txInfo = this.ac.getAionHub().getBlockchain().getTransactionInfo(txHash);

        if (txInfo == null) {
            return new RpcMsg(JSONObject.NULL);
        }

        Block block =
                this.ac.getAionHub().getBlockchain().getBlockByHash(txInfo.getBlockHash());

        if (block == null) {
            return new RpcMsg(JSONObject.NULL);
        }

        AionTransaction tx = txInfo.getReceipt().getTransaction();

        if (tx == null) {
            return new RpcMsg(JSONObject.NULL);
        }

        JSONObject result = new JSONObject();
        result.put("timestampVal", block.getTimestamp());
        result.put("transactionHash", StringUtils.toJsonHex(tx.getTransactionHash()));
        result.put("blockNumber", block.getNumber());
        result.put("blockHash", StringUtils.toJsonHex(block.getHash()));
        result.put("nonce", StringUtils.toJsonHex(tx.getNonce()));
        result.put("fromAddr", StringUtils.toJsonHex(tx.getSenderAddress().toByteArray()));
        result.put(
                "toAddr",
                StringUtils.toJsonHex(
                        tx.getDestinationAddress() == null
                                ? EMPTY_BYTE_ARRAY
                                : tx.getDestinationAddress().toByteArray()));
        result.put("value", StringUtils.toJsonHex(tx.getValue()));
        result.put("nrgPrice", tx.getEnergyPrice());
        result.put("nrgConsumed", txInfo.getReceipt().getEnergyUsed());
        result.put("data", StringUtils.toJsonHex(tx.getData()));
        result.put("transactionIndex", txInfo.getIndex());

        JSONArray logs = new JSONArray();
        for (Log l : txInfo.getReceipt().getLogInfoList()) {
            JSONObject log = new JSONObject();
            AionAddress aionAddress = new AionAddress(l.copyOfAddress());
            log.put("address", aionAddress.toString());
            log.put("data", StringUtils.toJsonHex(l.copyOfData()));
            JSONArray topics = new JSONArray();
            for (byte[] topic : l.copyOfTopics()) {
                topics.put(StringUtils.toJsonHex(topic));
            }
            log.put("topics", topics);
            logs.put(log);
        }
        result.put("logs", logs);

        return new RpcMsg(result);
    }

    public RpcMsg ops_getBlockDetailsByNumber(Object _params){
        String _blockNumber;
        if (_params instanceof JSONArray) {
            _blockNumber = ((JSONArray) _params).get(0) + "";
        } else if (_params instanceof JSONObject) {
            _blockNumber = ((JSONObject) _params).get("block") + "";
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }
        AionBlock block;
        Long bn = this.parseBnOrId(_blockNumber);

        // user passed a Long block number
        if (bn != null && bn >= 0) {
            block = (AionBlock) this.ac.getBlockchain().getBlockByNumber(bn);
            if (block == null) {
                return new RpcMsg(JSONObject.NULL);
            }
        }else {
            return new RpcMsg(JSONObject.NULL);
        }

        return rpcBlockDetailsFromBlock(block);
    }


    public RpcMsg ops_getBlockDetailsByHash(Object _params){
        String _blockHash;
        if (_params instanceof JSONArray) {
            _blockHash = ((JSONArray) _params).get(0) + "";
        } else if (_params instanceof JSONObject) {
            _blockHash = ((JSONObject) _params).get("block") + "";
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        Block block = this.ac.getBlockchain().getBlockByHash(ByteUtil.hexStringToBytes(_blockHash));

        if (block == null) {
                return new RpcMsg(JSONObject.NULL);
        }

        Block mainBlock = this.ac.getBlockchain().getBlockByNumber(block.getNumber());

        if (mainBlock == null || !Arrays.equals(block.getHash(), mainBlock.getHash())) {
            return new RpcMsg(JSONObject.NULL);
        }

        return rpcBlockDetailsFromBlock(block);
    }

    public RpcMsg ops_getBlock(Object _params) {
        String _bnOrHash;
        boolean _fullTx;
        if (_params instanceof JSONArray) {
            _bnOrHash = ((JSONArray) _params).get(0) + "";
            _fullTx = ((JSONArray) _params).optBoolean(1, false);
        } else if (_params instanceof JSONObject) {
            _bnOrHash = ((JSONObject) _params).get("block") + "";
            _fullTx = ((JSONObject) _params).optBoolean("fullTransaction", false);
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        AionBlock block = null;

        Long bn = this.parseBnOrId(_bnOrHash);

        // user passed a Long block number
        if (bn != null) {
            if (bn >= 0) {
                block = (AionBlock) this.ac.getBlockchain().getBlockByNumber(bn);
                if (block == null) {
                    return new RpcMsg(JSONObject.NULL);
                }
            } else {
                return new RpcMsg(JSONObject.NULL);
            }
        }

        // see if the user passed in a hash
        if (block == null) {
            block = (AionBlock) this.ac.getBlockchain().getBlockByHash(ByteUtil.hexStringToBytes(_bnOrHash));
            if (block == null) {
                return new RpcMsg(JSONObject.NULL);
            }
        }

        Block mainBlock = this.ac.getBlockchain().getBlockByNumber(block.getNumber());
        if (mainBlock == null) {
            return new RpcMsg(JSONObject.NULL);
        }

        if (!Arrays.equals(block.getHash(), mainBlock.getHash())) {
            return new RpcMsg(JSONObject.NULL);
        }

        // ok so now we have a mainchain block

        BigInteger blkReward =
                ((AionBlockchainImpl) ac.getBlockchain())
                        .getChainConfiguration()
                        .getRewardsCalculator()
                        .calculateReward(block.getHeader().getNumber());
        BigInteger totalDiff =
                this.ac.getAionHub().getBlockStore().getTotalDifficultyForHash(block.getHash());

        JSONObject blk = new JSONObject();
        blk.put("timestampVal", block.getTimestamp());
        blk.put("blockNumber", block.getNumber());
        blk.put("numTransactions", block.getTransactionsList().size());

        blk.put("blockHash", StringUtils.toJsonHex(block.getHash()));
        blk.put("parentHash", StringUtils.toJsonHex(block.getParentHash()));
        blk.put("minerAddress", StringUtils.toJsonHex(block.getCoinbase().toByteArray()));

        blk.put("receiptTxRoot", StringUtils.toJsonHex(block.getReceiptsRoot()));
        blk.put("txTrieRoot", StringUtils.toJsonHex(block.getTxTrieRoot()));
        blk.put("stateRoot", StringUtils.toJsonHex(block.getStateRoot()));

        blk.put("difficulty", StringUtils.toJsonHex(block.getDifficulty()));
        blk.put("totalDifficulty", totalDiff.toString(16));
        blk.put("nonce", StringUtils.toJsonHex(block.getNonce()));

        blk.put("blockReward", blkReward);
        blk.put("nrgConsumed", block.getNrgConsumed());
        blk.put("nrgLimit", block.getNrgLimit());

        blk.put("size", block.size());
        blk.put("bloom", StringUtils.toJsonHex(block.getLogBloom()));
        blk.put("extraData", StringUtils.toJsonHex(block.getExtraData()));
        blk.put("solution", StringUtils.toJsonHex(block.getHeader().getSolution()));

        JSONObject result = new JSONObject();
        result.put("blk", blk);

        if (_fullTx) {
            JSONArray txn = new JSONArray();
            for (AionTransaction tx : block.getTransactionsList()) {
                // transactionHash, fromAddr, toAddr, value, timestampVal, blockNumber, blockHash
                JSONArray t = new JSONArray();
                t.put(StringUtils.toJsonHex(tx.getTransactionHash()));
                t.put(StringUtils.toJsonHex(tx.getSenderAddress().toByteArray()));
                t.put(
                        StringUtils.toJsonHex(
                                tx.getDestinationAddress() == null
                                        ? EMPTY_BYTE_ARRAY
                                        : tx.getDestinationAddress().toByteArray()));
                t.put(StringUtils.toJsonHex(tx.getValue()));
                t.put(block.getTimestamp());
                t.put(block.getNumber());

                txn.put(t);
            }
            result.put("txn", txn);
        }

        return new RpcMsg(result);
    }

    /**
     * This function runs in ~ 30ms Is an order of magnitude slower than
     * ops_getTransactionReceiptByTransactionAndBlockHash
     */
    public RpcMsg ops_getTransactionReceiptByTransactionHash(Object _params) {
        String _transactionHash;
        if (_params instanceof JSONArray) {
            _transactionHash = ((JSONArray) _params).get(0) + "";
        } else if (_params instanceof JSONObject) {
            _transactionHash = ((JSONObject) _params).get("transactionHash") + "";
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        byte[] transactionHash = StringUtils.StringHexToByteArray(_transactionHash);

        AionTxInfo info = this.ac.getAionHub().getBlockchain().getTransactionInfo(transactionHash);
        if (info == null) {
            return new RpcMsg(JSONObject.NULL);
        }

        Block block = blockCache.get(new ByteArrayWrapper(info.getBlockHash()));

        return new RpcMsg((new TxRecpt(block, info, 0L, true)).toJson());
    }

    /**
     * This function runs as fast as is possible with the on-disk data model Use this to retrieve
     * the Transaction Receipt if you know the block hash already
     */
    public RpcMsg ops_getTransactionReceiptByTransactionAndBlockHash(Object _params) {
        String _transactionHash;
        String _blockHash;
        if (_params instanceof JSONArray) {
            _transactionHash = ((JSONArray) _params).get(0) + "";
            _blockHash = ((JSONArray) _params).get(1) + "";
        } else if (_params instanceof JSONObject) {
            _transactionHash = ((JSONObject) _params).get("transactionHash") + "";
            _blockHash = ((JSONObject) _params).get("blockHash") + "";
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        byte[] transactionHash = StringUtils.StringHexToByteArray(_transactionHash);
        byte[] blockHash = StringUtils.StringHexToByteArray(_blockHash);

        // cast will cause issues after the PoW refactor goes in
        AionBlockchainImpl chain = (AionBlockchainImpl) this.ac.getAionHub().getBlockchain();

        AionTxInfo info = chain.getTransactionInfoLite(transactionHash, blockHash);
        if (info == null) {
            return new RpcMsg(JSONObject.NULL);
        }

        Block block = blockCache.get(new ByteArrayWrapper(blockHash));

        AionTransaction t = block.getTransactionsList().get(info.getIndex());
        if (Arrays.compare(t.getTransactionHash(), transactionHash) != 0) {
            LOG.error("INCONSISTENT STATE: transaction info's transaction index is wrong.");
            return new RpcMsg(null, RpcError.INTERNAL_ERROR, "Database Error");
        }
        info.setTransaction(t);

        return new RpcMsg((new TxRecpt(block, info, 0L, true)).toJson());
    }

    public RpcMsg ops_getTransactionReceiptListByBlockHash(Object _params) {
        String _blockHash;
        if (_params instanceof JSONArray) {
            _blockHash = ((JSONArray) _params).get(0) + "";
        } else if (_params instanceof JSONObject) {
            _blockHash = ((JSONObject) _params).get("blockHash") + "";
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        byte[] blockHash = StringUtils.StringHexToByteArray(_blockHash);

        if (blockHash.length != 32) {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        // ok to getUnchecked() since the load() implementation does not throw checked exceptions
        Block b;
        try {
            b = blockCache.get(new ByteArrayWrapper(blockHash));
        } catch (Exception e) {
            // Catch errors if send an incorrect tx hash
            return new RpcMsg(
                    null,
                    RpcError.INVALID_REQUEST,
                    "Invalid Request " + e + " " + e.getMessage() != null ? e.getMessage() : "");
        }

        // cast will cause issues after the PoW refactor goes in
        AionBlockchainImpl chain = (AionBlockchainImpl) this.ac.getAionHub().getBlockchain();

        Function<AionTransaction, JSONObject> extractTxReceipt =
                t -> {
                    AionTxInfo info =
                            chain.getTransactionInfoLite(t.getTransactionHash(), b.getHash());
                    info.setTransaction(t);
                    return ((new TxRecpt(b, info, 0L, true)).toJson());
                };

        List<JSONObject> receipts;
        // use the fork-join pool to parallelize receipt retrieval if necessary
        int PARALLELIZE_RECEIPT_COUNT = 20;
        if (b.getTransactionsList().size() > PARALLELIZE_RECEIPT_COUNT) {
            receipts =
                    b.getTransactionsList()
                            .parallelStream()
                            .map(extractTxReceipt)
                            .collect(toList());
        } else {
            receipts = b.getTransactionsList().stream().map(extractTxReceipt).collect(toList());
        }

        return new RpcMsg(new JSONArray(receipts));
    }

    /* -------------------------------------------------------------------------
     * stratum pool
     */

    public RpcMsg stratum_getinfo() {
        JSONObject obj = new JSONObject();

        obj.put("balance", 0);
        obj.put("blocks", 0);
        obj.put("connections", peerCount());
        obj.put("proxy", "");
        obj.put("generate", true);
        obj.put("genproclimit", 100);
        obj.put("difficulty", 0);

        return new RpcMsg(obj);
    }

    public RpcMsg stratum_getwork() {
        // TODO: Change this to a synchronized map implementation mapping

        if (isSeedMode) {
            return new RpcMsg(null, RpcError.NOT_ALLOWED, "SeedNodeIsOpened");
        }

        BlockContext bestBlock = getBlockTemplate();
        ByteArrayWrapper key = new ByteArrayWrapper(bestBlock.block.getHeader().getMineHash());

        // Read template map; if block already contained chain has not moved forward, simply return
        // the same block.
        boolean isContained = false;
        try {
            templateMapLock.readLock().lock();
            if (templateMap.containsKey(key)) {
                isContained = true;
            }
        } finally {
            templateMapLock.readLock().unlock();
        }

        // Template not present in map; add it before returning
        if (!isContained) {
            try {
                templateMapLock.writeLock().lock();

                // Deep copy best block to avoid modifying internal best blocks
                bestBlock = new BlockContext(bestBlock);

                if (!templateMap.keySet().isEmpty()) {
                    if (templateMap.get(templateMap.keySet().iterator().next()).getNumber()
                            < bestBlock.block.getNumber()) {
                        // Found a higher block, clear any remaining cached entries and start on new
                        // height
                        templateMap.clear();
                    }
                }
                templateMap.put(key, bestBlock.block);

            } finally {
                templateMapLock.writeLock().unlock();
            }
        }

        JSONObject obj = new JSONObject();
        obj.put("previousblockhash", toHexString(bestBlock.block.getParentHash()));
        obj.put("height", bestBlock.block.getNumber());
        obj.put("target", toHexString(bestBlock.block.getHeader().getPowBoundary()));
        obj.put("headerHash", toHexString(bestBlock.block.getHeader().getMineHash()));
        obj.put("blockBaseReward", toHexString(bestBlock.baseBlockReward.toByteArray()));
        obj.put("blockTxFee", toHexString(bestBlock.transactionFee.toByteArray()));

        return new RpcMsg(obj);
    }

    public RpcMsg stratum_dumpprivkey() {
        return new RpcMsg("");
    }

    public RpcMsg stratum_validateaddress(Object _params) {
        /*
         * "isvalid" : true|false, (boolean) If the address is valid or not.
         * "address", (string) The aion address validated to ensure address is valid
         * address "ismine" : true|false, (boolean) If the address is contained in the keystore.
         */
        String _address;
        if (_params instanceof JSONArray) {
            _address = ((JSONArray) _params).get(0) + "";
        } else if (_params instanceof JSONObject) {
            _address = ((JSONObject) _params).get("address") + "";
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        JSONObject obj = new JSONObject();

        obj.put("isvalid", StringUtils.isValidAddress(_address));
        obj.put("address", _address + "");
        obj.put("ismine", Keystore.exist(_address));
        return new RpcMsg(obj);
    }

    public RpcMsg stratum_getdifficulty() {
        /*
         * Return the highest known difficulty
         */
        return new RpcMsg(getBestBlock().getDifficultyBI().toString(16));
    }

    public RpcMsg stratum_getmininginfo() {
        // TODO: [Unity] This cast should be removed when we support staking blocks
        AionBlock bestBlock = (AionBlock) getBestBlock();

        JSONObject obj = new JSONObject();
        obj.put("blocks", bestBlock.getNumber());
        obj.put("currentblocksize", bestBlock.size());
        obj.put("currentblocktx", bestBlock.getTransactionsList().size());
        obj.put("difficulty", bestBlock.getDifficultyBI().toString(16));
        obj.put("testnet", true);

        return new RpcMsg(obj);
    }

    public RpcMsg stratum_submitblock(Object _params) {
        Object nce;
        Object soln;
        Object hdrHash;
        if (_params instanceof JSONArray) {
            nce = ((JSONArray) _params).opt(0);
            soln = ((JSONArray) _params).opt(1);
            hdrHash = ((JSONArray) _params).opt(2);
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        JSONObject obj = new JSONObject();

        if (!JSONObject.NULL.equals(nce)
                && !JSONObject.NULL.equals(soln)
                && !JSONObject.NULL.equals(hdrHash)) {
            try {
                templateMapLock.writeLock().lock();

                ByteArrayWrapper key = new ByteArrayWrapper(hexStringToBytes((String) hdrHash));

                // Grab copy of best block
                AionBlock bestBlock = templateMap.get(key);
                if (bestBlock != null) {
                    bestBlock.getHeader().setSolution(hexStringToBytes(soln + ""));
                    bestBlock.getHeader().setNonce(hexStringToBytes(nce + ""));

                    // Directly submit to chain for new due to delays using event, explore event
                    // submission again
                    ImportResult importResult = AionImpl.inst().addNewMinedBlock(bestBlock);
                    if (importResult.isSuccessful()) {
                        templateMap.remove(key);
                        LOG.info(
                                "block submitted via api <num={}, hash={}, diff={}, tx={}>",
                                bestBlock.getNumber(),
                                bestBlock.getShortHash(), // LogUtil.toHexF8(newBlock.getHash()),
                                bestBlock.getHeader().getDifficultyBI().toString(),
                                bestBlock.getTransactionsList().size());
                    } else {
                        LOG.info(
                                "Unable to submit block via api <num={}, hash={}, diff={}, tx={}>",
                                bestBlock.getNumber(),
                                bestBlock.getShortHash(), // LogUtil.toHexF8(newBlock.getHash()),
                                bestBlock.getHeader().getDifficultyBI().toString(),
                                bestBlock.getTransactionsList().size());
                    }
                }
            } finally {
                templateMapLock.writeLock().unlock();
            }

            // TODO: Simplified response for now, need to provide better feedback to caller in next
            // update
            obj.put("result", true);
        } else {
            obj.put("message", "success");
            obj.put("code", -1);
        }

        return new RpcMsg(obj);
    }

    public RpcMsg stratum_getHeaderByBlockNumber(Object _params) {
        Object _blockNum;
        if (_params instanceof JSONArray) {
            _blockNum = ((JSONArray) _params).opt(0);
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        JSONObject obj = new JSONObject();

        if (!JSONObject.NULL.equals(_blockNum)) {
            String bnStr = _blockNum + "";
            try {
                int bnInt = Integer.decode(bnStr);
                // TODO: [Unity] This cast should be removed when we support staking blocks
                AionBlock block = (AionBlock) getBlockRaw(bnInt);
                if (block != null) {
                    BlockHeader header = block.getHeader();
                    obj.put("code", 0); // 0 = success
                    obj.put("nonce", toHexString(header.getNonce()));
                    obj.put("solution", toHexString(header.getSolution()));
                    obj.put("headerHash", toHexString(header.getMineHash()));
                    obj.putOpt("blockHeader", header.toJSON());
                } else {
                    obj.put("message", "Fail - Unable to find block" + bnStr);
                    obj.put("code", -2);
                }
            } catch (Exception e) {
                obj.put("message", bnStr + " must be an integer value");
                obj.put("code", -3);
            }
        } else {
            obj.put("message", "Missing block number");
            obj.put("code", -1);
        }

        return new RpcMsg(obj);
    }

    // always gets the latest 20 blocks and transactions
    private class MinerStatsView {

        LinkedList<byte[]> hashQueue; // more precisely a dequeue
        Map<byte[], Block> blocks;
        private JSONObject response;
        private int qSize;
        private byte[] miner;

        MinerStatsView(MinerStatsView cv) {
            hashQueue = new LinkedList<>(cv.hashQueue);
            blocks = new HashMap<>(cv.blocks);
            response = new JSONObject(cv.response, JSONObject.getNames(cv.response));
            qSize = cv.qSize;
            miner = cv.miner;
        }

        MinerStatsView(int _qSize, byte[] _miner) {
            hashQueue = new LinkedList<>();
            blocks = new HashMap<>();
            response = new JSONObject();
            qSize = _qSize;
            miner = _miner;
        }

        private JSONObject buildResponse() {
            BigInteger lastDifficulty = BigInteger.ZERO;
            long blkTimeAccumulator = 0;
            long minedCount = 0L;

            int minedByMiner = 0;

            double minerHashrateShare = 0;
            BigDecimal minerHashrate = BigDecimal.ZERO;
            BigDecimal networkHashrate = BigDecimal.ZERO;

            int blkTimesAccumulated = 0;
            Long lastBlkTimestamp = null;
            Block b = null;

            try {
                // index 0 = latest block
                int i = 0;
                ListIterator li = hashQueue.listIterator(0);
                while (li.hasNext()) {
                    byte[] hash = (byte[]) li.next();
                    b = blocks.get(hash);

                    if (i == 0) {
                        lastDifficulty = b.getDifficultyBI();
                    }

                    // only accumulate block times over the last 32 blocks
                    if (i <= STRATUM_BLKTIME_INCLUDED_COUNT) {
                        if (lastBlkTimestamp != null) {
                            //                            System.out.println("blocktime for [" +
                            // b.getNumber() + "] = " + (lastBlkTimestamp - b.getTimestamp()));
                            blkTimeAccumulator += lastBlkTimestamp - b.getTimestamp();
                            blkTimesAccumulated++;
                        }
                        lastBlkTimestamp = b.getTimestamp();
                    }

                    if (Arrays.equals(b.getCoinbase().toByteArray(), miner)) {
                        minedByMiner++;
                    }

                    i++;
                }

                double blkTime = 0L;
                if (blkTimesAccumulated > 0) {
                    blkTime = blkTimeAccumulator / (double) blkTimesAccumulated;
                }

                if (blkTime > 0) {
                    networkHashrate =
                            (new BigDecimal(lastDifficulty))
                                    .divide(BigDecimal.valueOf(blkTime), 4, RoundingMode.HALF_UP);
                }

                if (i > 0) {
                    minerHashrateShare = minedByMiner / (double) i;
                }

                minerHashrate = BigDecimal.valueOf(minerHashrateShare).multiply(networkHashrate);

            } catch (Exception e) {
                LOG.error("failed to compute miner metrics", e);
            }

            JSONObject o = new JSONObject();
            o.put("networkHashrate", networkHashrate.toString());
            o.put("minerHashrate", minerHashrate.toString());
            o.put("minerHashrateShare", minerHashrateShare);
            return o;
        }

        MinerStatsView update() {
            // get the latest head
            Block blk = getBestBlock();

            if (blk == null) {
                return this;
            }

            if (Arrays.equals(hashQueue.peekFirst(), blk.getHash())) {
                return this; // nothing to do
            }

            // evict data as necessary
            LinkedList<Map.Entry<byte[], Block>> tempStack = new LinkedList<>();
            tempStack.push(Map.entry(blk.getHash(), blk));
            int itr = 1; // deliberately 1, since we've already added the 0th element to the stack

            /*
            if (hashQueue.peekFirst() != null) {
                System.out.println("[" + 0 + "]: " + StringUtils.toJsonHex(hashQueue.peekFirst()) + " - " + blocks.get(hashQueue.peekFirst()).getNumber());
                System.out.println("----------------------------------------------------------");
                System.out.println("isParentHashMatch? " + FastByteComparisons.equal(hashQueue.peekFirst(), blk.getParentHash()));
                System.out.println("blk.getNumber() " + blk.getNumber());
            }
            System.out.println("blkNum: " + blk.getNumber() +
                    " parentHash: " + StringUtils.toJsonHex(blk.getParentHash()) +
                    " blkHash: " + StringUtils.toJsonHex(blk.getHash()));
            */

            while (!Arrays.equals(hashQueue.peekFirst(), blk.getParentHash())
                    && itr < qSize
                    && blk.getNumber() > 2) {

                blk = getBlockByHash(blk.getParentHash());
                tempStack.push(Map.entry(blk.getHash(), blk));
                itr++;
                /*
                System.out.println("blkNum: " + blk.getNumber() +
                        " parentHash: " + StringUtils.toJsonHex(blk.getParentHash()) +
                        " blkHash: " + StringUtils.toJsonHex(blk.getHash()));
                */
            }

            // evict out the right number of elements first
            for (int i = 0; i < tempStack.size(); i++) {
                byte[] tailHash = hashQueue.pollLast();
                if (tailHash != null) {
                    blocks.remove(tailHash);
                }
            }

            // empty out the stack into the queue
            while (!tempStack.isEmpty()) {
                // add to the queue
                Map.Entry<byte[], Block> element = tempStack.pop();
                byte[] hash = element.getKey();
                Block blkObj = element.getValue();

                hashQueue.push(hash);
                blocks.put(hash, blkObj);
            }

            /*
            System.out.println("[" + 0 + "]: " + StringUtils.toJsonHex(hashQueue.peekFirst()) + " - " + blocks.get(hashQueue.peekFirst()).getNumber());
            System.out.println("----------------------------------------------------------");
            for (int i = hashQueue.size() - 1; i >= 0; i--) {
                System.out.println("[" + i + "]: " + StringUtils.toJsonHex(hashQueue.get(i)) + " - " + blocks.get(hashQueue.get(i)).getNumber());
            }
            */
            this.response = buildResponse();

            return this;
        }

        JSONObject getResponse() {
            return response;
        }
    }

    public RpcMsg stratum_getMinerStats(Object _params) {
        String _address;
        if (_params instanceof JSONArray) {
            _address = ((JSONArray) _params).get(0) + "";
        } else if (_params instanceof JSONObject) {
            _address = ((JSONObject) _params).get("address") + "";
        } else {
            return new RpcMsg(null, RpcError.INVALID_PARAMS, "Invalid parameters");
        }

        try {
            MinerStatsView v = MinerStats.get(_address);
            return new RpcMsg(v.getResponse());
        } catch (Exception e) {
            LOG.error("<rpc-server - cannot get cached response for stratum_getMinerStats: ", e);
            return new RpcMsg(null, RpcError.EXECUTION_ERROR, "Cached response retrieve failed.");
        }
    }

    // --------------------------------------------------------------------
    // Helper Functions
    // --------------------------------------------------------------------

    // potential bug introduced by .getSnapshotTo()
    // comment out until resolved
    private Repository getRepoByJsonBlockId(String _bnOrId) {
        Long bn = parseBnOrId(_bnOrId);

        if (bn == null) {
            return null;
        }

        if (bn == BEST_PENDING_BLOCK) return pendingState.getRepository();

        Block b = this.ac.getBlockchain().getBlockByNumber(bn);
        if (b == null) {
            return null;
        }

        return ac.getRepository().getSnapshotTo(b.getStateRoot());
    }

    private Block getBlockByBN(long bn) {
        if (bn == BEST_PENDING_BLOCK) {
            return pendingState.getBestBlock();
        } else {
            return this.ac.getBlockchain().getBlockByNumber(bn);
        }
    }

    // Note: If return is null, caller sometimes assumes no blockNumber was passed in

    private Long parseBnOrId(String _bnOrId) {
        if (_bnOrId == null) {
            return null;
        }

        try {
            if ("earliest".equalsIgnoreCase(_bnOrId)) {
                return 0L;
            } else if ("latest".equalsIgnoreCase(_bnOrId)) {
                return getBestBlock().getNumber();
            } else if ("pending".equalsIgnoreCase(_bnOrId)) {
                return BEST_PENDING_BLOCK;
            } else {

                Long ret;

                if (_bnOrId.startsWith("0x")) {
                    ret = StringUtils.StringHexToBigInteger(_bnOrId).longValue();
                } else {
                    ret = Long.parseLong(_bnOrId);
                }
                if (ret < 0) {
                    LOG.debug("block number cannot be negative" + _bnOrId);
                    return null;
                } else {
                    return ret;
                }
            }
        } catch (Exception e) {
            LOG.debug("err on parsing block number #" + _bnOrId);
            return null;
        }
    }


    private RpcMsg rpcBlockDetailsFromBlock(Block block){

        BigInteger blkReward =
            ((AionBlockchainImpl) ac.getBlockchain())
                .getChainConfiguration()
                .getRewardsCalculator()
                .calculateReward(block.getHeader().getNumber());
        BigInteger totalDiff =
            this.ac.getAionHub().getBlockStore().getTotalDifficultyForHash(block.getHash());

        List<AionTxInfo> txInfoList = new ArrayList<>();
        for (AionTransaction transaction: block.getTransactionsList()){
            AionTxInfo txInfo = this.ac.getAionHub()
                .getBlockchain()
                .getTransactionInfo(transaction.getTransactionHash());
            txInfoList.add(txInfo);
        }
        Block previousBlock = this.ac.getBlockchain().getBlockByHash(block.getParentHash());
        // get the parent block

        Long previousTimestamp;

        if (previousBlock == null){
            previousTimestamp = null;
        }
        else {
            previousTimestamp = previousBlock.getTimestamp();
        }

        return new RpcMsg(
            Blk.aionBlockDetailsToJson(block, txInfoList, previousTimestamp, totalDiff, blkReward
            ));
    }

    public void shutdown() {
        destroyCaches();
        if (isFilterEnabled) {
            shutDownES();
        }
    }
}
