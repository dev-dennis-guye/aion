package org.aion.zero.impl.sync;

import static org.aion.util.string.StringUtils.getNodeIdShort;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.aion.evtmgr.IEvent;
import org.aion.evtmgr.IEventMgr;
import org.aion.evtmgr.impl.evt.EventConsensus;
import org.aion.log.AionLoggerFactory;
import org.aion.log.LogEnum;
import org.aion.mcf.blockchain.Block;
import org.aion.mcf.blockchain.BlockHeader;
import org.aion.mcf.config.StatsType;
import org.aion.p2p.IP2pMgr;
import org.aion.util.bytes.ByteUtil;
import org.aion.util.conversions.Hex;
import org.aion.util.types.ByteArrayWrapper;
import org.aion.zero.impl.blockchain.AionBlockchainImpl;
import org.aion.zero.impl.blockchain.ChainConfiguration;
import org.aion.zero.impl.types.AionBlock;
import org.aion.zero.impl.valid.BlockHeaderValidator;
import org.apache.commons.collections4.map.LRUMap;
import org.slf4j.Logger;

/** @author chris */
public final class SyncMgr {

    // interval - show status
    private static final int INTERVAL_SHOW_STATUS = 10000;

    private static final Logger log = AionLoggerFactory.getLogger(LogEnum.SYNC.name());
    private static final Logger survey_log = AionLoggerFactory.getLogger(LogEnum.SURVEY.name());

    private final NetworkStatus networkStatus = new NetworkStatus();
    // peer syncing states
    private final Map<Integer, PeerState> peerStates = new ConcurrentHashMap<>();
    // store the downloaded headers from network
    private final BlockingQueue<HeadersWrapper> downloadedHeaders = new LinkedBlockingQueue<>();
    // store the headers whose bodies have been requested from corresponding peer
    private final ConcurrentHashMap<Integer, HeadersWrapper> headersWithBodiesRequested =
            new ConcurrentHashMap<>();
    // store the downloaded blocks that are ready to import
    private final BlockingQueue<BlocksWrapper> downloadedBlocks = new LinkedBlockingQueue<>();
    // store the hashes of blocks which have been successfully imported
    private final Map<ByteArrayWrapper, Object> importedBlockHashes =
            Collections.synchronizedMap(new LRUMap<>(4096));
    private int blocksQueueMax; // block header wrappers
    private AionBlockchainImpl chain;
    private IP2pMgr p2pMgr;
    private IEventMgr evtMgr;
    private SyncStats stats;
    private AtomicBoolean start = new AtomicBoolean(true);
    // private ExecutorService workers = Executors.newFixedThreadPool(5);
    private ExecutorService workers =
            Executors.newCachedThreadPool(
                    new ThreadFactory() {

                        private AtomicInteger cnt = new AtomicInteger(0);

                        @Override
                        public Thread newThread(Runnable r) {
                            return new Thread(r, "sync-gh-" + cnt.incrementAndGet());
                        }
                    });

    private Thread syncGb = null;
    private Thread syncIb = null;
    private Thread syncGs = null;
    private Thread syncSs = null;

    private BlockHeaderValidator blockHeaderValidator;
    private volatile long timeUpdated = 0;
    private AtomicBoolean queueFull = new AtomicBoolean(false);

    public static SyncMgr inst() {
        return AionSyncMgrHolder.INSTANCE;
    }

    /**
     * @param _displayId String
     * @param _remoteBestBlockNumber long
     * @param _remoteBestBlockHash byte[]
     * @param _remoteTotalDiff BigInteger null check for _remoteBestBlockHash && _remoteTotalDiff
     *     implemented on ResStatusHandler before pass through
     */
    public void updateNetworkStatus(
            String _displayId,
            long _remoteBestBlockNumber,
            final byte[] _remoteBestBlockHash,
            BigInteger _remoteTotalDiff,
            byte _apiVersion,
            short _peerCount,
            int _pendingTxCount,
            int _latency) {

        // self
        BigInteger selfTd = this.chain.getTotalDifficulty();

        // trigger send headers routine immediately
        if (_remoteTotalDiff.compareTo(selfTd) > 0) {
            this.getHeaders(selfTd);
        }

        long now = System.currentTimeMillis();
        if ((now - timeUpdated) > 1000) {
            timeUpdated = now;
            // update network best status
            synchronized (this.networkStatus) {
                BigInteger networkTd = this.networkStatus.getTargetTotalDiff();
                if (_remoteTotalDiff.compareTo(networkTd) > 0) {
                    String remoteBestBlockHash = Hex.toHexString(_remoteBestBlockHash);

                    if (log.isDebugEnabled()) {
                        log.debug(
                                "network-status-updated on-sync id={}->{} td={}->{} bn={}->{} bh={}->{}",
                                this.networkStatus.getTargetDisplayId(),
                                _displayId,
                                this.networkStatus.getTargetTotalDiff().toString(10),
                                _remoteTotalDiff.toString(10),
                                this.networkStatus.getTargetBestBlockNumber(),
                                _remoteBestBlockNumber,
                                this.networkStatus.getTargetBestBlockHash().isEmpty()
                                        ? ""
                                        : getNodeIdShort(
                                                this.networkStatus.getTargetBestBlockHash()),
                                getNodeIdShort(remoteBestBlockHash),
                                this.networkStatus.getTargetApiVersion(),
                                (int) _apiVersion,
                                this.networkStatus.getTargetPeerCount(),
                                _peerCount,
                                this.networkStatus.getTargetPendingTxCount(),
                                _pendingTxCount,
                                this.networkStatus.getTargetLatency(),
                                _latency);
                    }

                    this.networkStatus.update(
                            _displayId,
                            _remoteTotalDiff,
                            _remoteBestBlockNumber,
                            remoteBestBlockHash,
                            (int) _apiVersion,
                            _peerCount,
                            _pendingTxCount,
                            _latency);
                }
            }
        }
    }

    public void init(
            final AionBlockchainImpl _chain,
            final IP2pMgr _p2pMgr,
            final IEventMgr _evtMgr,
            final int _blocksQueueMax,
            final boolean _showStatus,
            final Set<StatsType> showStatistics,
            final int _slowImportTime,
            final int _compactFrequency,
            final int maxActivePeers) {
        p2pMgr = _p2pMgr;
        chain = _chain;
        evtMgr = _evtMgr;

        blocksQueueMax = _blocksQueueMax;

        blockHeaderValidator = new ChainConfiguration().createBlockHeaderValidator();

        long selfBest = chain.getBestBlock().getNumber();
        stats = new SyncStats(selfBest, _showStatus, showStatistics, maxActivePeers);

        syncGb =
                new Thread(
                        new TaskGetBodies(
                                p2pMgr,
                                start,
                                downloadedHeaders,
                                headersWithBodiesRequested,
                                peerStates,
                                stats,
                                log),
                        "sync-gb");
        syncGb.start();
        syncIb =
                new Thread(
                        new TaskImportBlocks(
                                log,
                                survey_log,
                                chain,
                                start,
                                stats,
                                downloadedBlocks,
                                importedBlockHashes,
                                peerStates,
                                _slowImportTime,
                                _compactFrequency),
                        "sync-ib");
        syncIb.start();
        syncGs = new Thread(new TaskGetStatus(start, p2pMgr, stats, log), "sync-gs");
        syncGs.start();

        if (_showStatus) {
            syncSs =
                    new Thread(
                            new TaskShowStatus(
                                    start,
                                    INTERVAL_SHOW_STATUS,
                                    chain,
                                    networkStatus,
                                    stats,
                                    p2pMgr,
                                    peerStates,
                                    showStatistics,
                                    AionLoggerFactory.getLogger(LogEnum.P2P.name())),
                            "sync-ss");
            syncSs.start();
        }

        setupEventHandler();
    }

    private void setupEventHandler() {
        List<IEvent> events = new ArrayList<>();
        events.add(new EventConsensus(EventConsensus.CALLBACK.ON_SYNC_DONE));
        this.evtMgr.registerEvent(events);
    }

    private void getHeaders(BigInteger _selfTd) {
        if (downloadedBlocks.size() > blocksQueueMax) {
            if (queueFull.compareAndSet(false, true)) {
                log.debug("Downloaded blocks queue is full. Stop requesting headers");
            }
        } else {
            if (!workers.isShutdown()) {
                workers.submit(
                        new TaskGetHeaders(
                                p2pMgr,
                                chain.getBestBlock().getNumber(),
                                _selfTd,
                                peerStates,
                                stats,
                                log));
                queueFull.set(false);
            }
        }
    }

    /**
     * @param _nodeIdHashcode int
     * @param _displayId String
     * @param _headers List validate headers batch and add batch to imported headers
     */
    public void validateAndAddHeaders(
            int _nodeIdHashcode, String _displayId, List<BlockHeader> _headers) {
        if (_headers == null || _headers.isEmpty()) {
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug(
                    "<incoming-headers from={} size={} node={}>",
                    _headers.get(0).getNumber(),
                    _headers.size(),
                    _displayId);
        }

        // filter imported block headers
        List<BlockHeader> filtered = new ArrayList<>();
        BlockHeader prev = null;
        for (BlockHeader current : _headers) {

            // ignore this batch if any invalidated header
            if (!this.blockHeaderValidator.validate(current, log)) {
                log.debug(
                        "<invalid-header num={} hash={}>", current.getNumber(), current.getHash());

                // Print header to allow debugging
                log.debug("Invalid header: {}", current.toString());

                return;
            }

            // break if not consisting
            if (prev != null
                    && (current.getNumber() != (prev.getNumber() + 1)
                            || !Arrays.equals(current.getParentHash(), prev.getHash()))) {
                log.debug(
                        "<inconsistent-block-headers from={}, num={}, prev+1={}, p_hash={}, prev={}>",
                        _displayId,
                        current.getNumber(),
                        prev.getNumber() + 1,
                        ByteUtil.toHexString(current.getParentHash()),
                        ByteUtil.toHexString(prev.getHash()));
                return;
            }

            // add if not cached
            if (!importedBlockHashes.containsKey(ByteArrayWrapper.wrap(current.getHash()))) {
                filtered.add(current);
            }

            prev = current;
        }

        // NOTE: the filtered headers is still continuous

        if (!filtered.isEmpty()) {
            downloadedHeaders.add(new HeadersWrapper(_nodeIdHashcode, _displayId, filtered));
        }
    }

    /**
     * @param _nodeIdHashcode int
     * @param _displayId String
     * @param _bodies List<byte[]> Assemble and validate blocks batch and add batch to import queue
     *     from network response blocks bodies
     */
    public void validateAndAddBlocks(
            int _nodeIdHashcode, String _displayId, final List<byte[]> _bodies) {

        HeadersWrapper hw = this.headersWithBodiesRequested.remove(_nodeIdHashcode);
        if (hw == null || _bodies == null) {
            return;
        }

        // assemble batch
        List<BlockHeader> headers = hw.getHeaders();
        List<Block> blocks = new ArrayList<>(_bodies.size());
        Iterator<BlockHeader> headerIt = headers.iterator();
        Iterator<byte[]> bodyIt = _bodies.iterator();
        while (headerIt.hasNext() && bodyIt.hasNext()) {
            AionBlock block = AionBlock.createBlockFromNetwork(headerIt.next(), bodyIt.next());
            if (block == null) {
                log.error("<assemble-and-validate-blocks node={}>", _displayId);
                break;
            } else {
                blocks.add(block);
            }
        }

        int m = blocks.size();
        if (m == 0) {
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug(
                    "<incoming-bodies from={} size={} node={}>",
                    blocks.get(0).getNumber(),
                    blocks.size(),
                    _displayId);
        }

        // add batch
        downloadedBlocks.add(new BlocksWrapper(_nodeIdHashcode, _displayId, blocks));
    }

    public long getNetworkBestBlockNumber() {
        synchronized (this.networkStatus) {
            return this.networkStatus.getTargetBestBlockNumber();
        }
    }

    public synchronized void shutdown() {
        start.set(false);
        workers.shutdown();

        interruptAndWait(syncGb, 10000);
        interruptAndWait(syncIb, 10000);
        interruptAndWait(syncGs, 10000);
        interruptAndWait(syncSs, 10000);
    }

    private void interruptAndWait(Thread t, long timeout) {
        if (t != null) {
            log.info("Stopping thread: " + t.getName());
            t.interrupt();
            try {
                t.join(timeout);
            } catch (InterruptedException e) {
                log.warn("Failed to stop " + t.getName());
            }
        }
    }

    public Map<Integer, PeerState> getPeerStates() {
        return new HashMap<>(this.peerStates);
    }

    public SyncStats getSyncStats() {
        return this.stats;
    }

    private static final class AionSyncMgrHolder {
        static final SyncMgr INSTANCE = new SyncMgr();
    }
}
