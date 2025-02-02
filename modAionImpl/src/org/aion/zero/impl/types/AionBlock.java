package org.aion.zero.impl.types;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.aion.base.AionTransaction;
import org.aion.base.TxUtil;
import org.aion.log.AionLoggerFactory;
import org.aion.log.LogEnum;
import org.aion.mcf.blockchain.Block;
import org.aion.mcf.blockchain.BlockHeader;
import org.aion.zero.impl.trie.Trie;
import org.aion.zero.impl.trie.TrieImpl;
import org.aion.rlp.RLP;
import org.aion.rlp.RLPElement;
import org.aion.rlp.RLPList;
import org.aion.types.AionAddress;
import org.aion.util.bytes.ByteUtil;
import org.aion.util.conversions.Hex;
import org.aion.util.types.ByteArrayWrapper;
import org.aion.zero.impl.exceptions.HeaderStructureException;
import org.slf4j.Logger;

/** */
public class AionBlock extends AbstractBlock implements Block {

    private static final Logger LOG = AionLoggerFactory.getLogger(LogEnum.CONS.toString());

    /* Private */
    private byte[] rlpEncoded;
    private volatile boolean parsed = false;
    private BigInteger td = null;
    private A0BlockHeader header;

    /* Constructors */
    private AionBlock() {}

    // copy constructor
    public AionBlock(AionBlock block) {
        this.header = new A0BlockHeader(block.getHeader());
        this.transactionsList.addAll(block.getTransactionsList());
        this.parsed = true;
    }

    public AionBlock(byte[] rawData) {
        this.rlpEncoded = rawData;
    }

    /**
     * Returns a new block that contains zero transactions and whose block header is the empty
     * header that is equal to the header generated by {@link A0BlockHeader.Builder#build()} .
     *
     * @return A new empty block.
     */
    public static AionBlock newEmptyBlock() {
        return new AionBlock(new A0BlockHeader.Builder().build(), Collections.emptyList());
    }

    /**
     * All construction using this codepath leads from DB queries or creation of new blocks
     *
     * @implNote do not use this construction path for unsafe sources
     */
    public AionBlock(A0BlockHeader header, List<AionTransaction> transactionsList) {
        this(
                header.getParentHash(),
                header.getCoinbase(),
                header.getLogsBloom(),
                header.getDifficulty(),
                header.getNumber(),
                header.getTimestamp(),
                header.getExtraData(),
                header.getNonce(),
                header.getReceiptsRoot(),
                header.getTxTrieRoot(),
                header.getStateRoot(),
                transactionsList,
                header.getSolution(),
                header.getEnergyConsumed(),
                header.getEnergyLimit());
    }

    public AionBlock(
            byte[] parentHash,
            AionAddress coinbase,
            byte[] logsBloom,
            byte[] difficulty,
            long number,
            long timestamp,
            byte[] extraData,
            byte[] nonce,
            byte[] receiptsRoot,
            byte[] transactionsRoot,
            byte[] stateRoot,
            List<AionTransaction> transactionsList,
            byte[] solutions,
            long energyConsumed,
            long energyLimit) {

        A0BlockHeader.Builder builder = new A0BlockHeader.Builder();

        try {
            builder.withParentHash(parentHash)
                    .withCoinbase(coinbase)
                    .withLogsBloom(logsBloom)
                    .withDifficulty(difficulty)
                    .withNumber(number)
                    .withTimestamp(timestamp)
                    .withExtraData(extraData)
                    .withNonce(nonce)
                    .withReceiptTrieRoot(receiptsRoot)
                    .withTxTrieRoot(transactionsRoot)
                    .withStateRoot(stateRoot)
                    .withSolution(solutions)
                    .withEnergyConsumed(energyConsumed)
                    .withEnergyLimit(energyLimit);
        } catch (HeaderStructureException e) {
            throw new RuntimeException(e);
        }

        this.header = builder.build();
        this.transactionsList =
                transactionsList == null ? new CopyOnWriteArrayList<>() : transactionsList;
        this.parsed = true;
    }

    /**
     * Constructor used in genesis creation, note that although genesis does check whether the
     * fields are correct, we emit a checked exception if in some unforseen circumstances we deem
     * the fields as incorrect
     */
    protected AionBlock(
            byte[] parentHash,
            AionAddress coinbase,
            byte[] logsBloom,
            byte[] difficulty,
            long number,
            long timestamp,
            byte[] extraData,
            byte[] nonce,
            long energyLimit)
            throws HeaderStructureException {
        A0BlockHeader.Builder builder = new A0BlockHeader.Builder();
        builder.withParentHash(parentHash)
                .withCoinbase(coinbase)
                .withLogsBloom(logsBloom)
                .withDifficulty(difficulty)
                .withNumber(number)
                .withTimestamp(timestamp)
                .withExtraData(extraData)
                .withNonce(nonce)
                .withEnergyLimit(energyLimit);
        this.header = builder.build();
        this.parsed = true;
    }

    public void parseRLP() {
        if (this.parsed) {
            return;
        }

        synchronized (this) {
            if (this.parsed) return;

            RLPList params = RLP.decode2(rlpEncoded);
            RLPList block = (RLPList) params.get(0);

            // Parse Header
            RLPList header = (RLPList) block.get(0);
            this.header = new A0BlockHeader(header);

            // Parse Transactions
            RLPList txTransactions = (RLPList) block.get(1);
            this.parseTxs(this.header.getTxTrieRoot(), txTransactions);

            this.parsed = true;
        }
    }

    public int size() {
        return getEncoded().length;
    }

    public A0BlockHeader getHeader() {
        parseRLP();
        return this.header;
    }

    public byte[] getHash() {
        parseRLP();
        return this.header.getHash();
    }

    public byte[] getParentHash() {
        parseRLP();
        return this.header.getParentHash();
    }

    public AionAddress getCoinbase() {
        parseRLP();
        return this.header.getCoinbase();
    }

    @Override
    public byte[] getStateRoot() {
        parseRLP();
        return this.header.getStateRoot();
    }

    @Override
    public void setStateRoot(byte[] stateRoot) {
        parseRLP();
        this.header.setStateRoot(stateRoot);
    }

    public byte[] getTxTrieRoot() {
        parseRLP();
        return this.header.getTxTrieRoot();
    }

    public byte[] getReceiptsRoot() {
        parseRLP();
        return this.header.getReceiptsRoot();
    }

    public byte[] getLogBloom() {
        parseRLP();
        return this.header.getLogsBloom();
    }

    @Override
    public byte[] getDifficulty() {
        parseRLP();
        return this.header.getDifficulty();
    }

    public BigInteger getDifficultyBI() {
        parseRLP();
        return this.header.getDifficultyBI();
    }

    public BigInteger getCumulativeDifficulty() {
        if (td == null) {
            return BigInteger.ZERO;
        } else {
            return td;
        }
    }

    public long getTimestamp() {
        parseRLP();
        return this.header.getTimestamp();
    }

    @Override
    public long getNumber() {
        parseRLP();
        return this.header.getNumber();
    }

    public byte[] getExtraData() {
        parseRLP();
        return this.header.getExtraData();
    }

    public byte[] getNonce() {
        parseRLP();
        return this.header.getNonce();
    }

    public void setNonce(byte[] nonce) {
        this.getHeader().setNonce(nonce);
        rlpEncoded = null;
    }

    public void setExtraData(byte[] data) {
        this.getHeader().setExtraData(data);
        rlpEncoded = null;
    }

    public List<AionTransaction> getTransactionsList() {
        parseRLP();
        return transactionsList;
    }

    // used to reduce the number of times we create equal wrapper objects
    private ByteArrayWrapper hashWrapper = null;
    private ByteArrayWrapper parentHashWrapper = null;

    /**
     * Returns a {@link ByteArrayWrapper} instance of the block's hash.
     *
     * @return a {@link ByteArrayWrapper} instance of the block's hash
     * @implNote Not safe when the block is mutable. Use this only for sync where the block's hash
     *     cannot change during execution.
     */
    public ByteArrayWrapper getHashWrapper() {
        if (hashWrapper == null) {
            hashWrapper = ByteArrayWrapper.wrap(getHash());
        }
        return hashWrapper;
    }

    /**
     * Returns a {@link ByteArrayWrapper} instance of the block's parent hash.
     *
     * @return a {@link ByteArrayWrapper} instance of the block's parent hash
     * @implNote Not safe when the block is mutable. Use this only for sync where the block's parent
     *     hash cannot change during execution.
     */
    public ByteArrayWrapper getParentHashWrapper() {
        if (parentHashWrapper == null) {
            parentHashWrapper = ByteArrayWrapper.wrap(getParentHash());
        }
        return parentHashWrapper;
    }

    /**
     * Facilitates the "finalization" of the block, after processing the necessary transactions.
     * This will be called during block creation and is considered the last step conducted by the
     * blockchain before handing it off to miner. This step is necessary to add post-execution
     * states:
     *
     * <p>{@link A0BlockHeader#txTrieRoot} {@link A0BlockHeader#receiptTrieRoot} {@link
     * A0BlockHeader#stateRoot} {@link A0BlockHeader#logsBloom} {@link this#transactionsList} {@link
     * A0BlockHeader#energyConsumed}
     *
     * <p>The (as of now) unenforced contract by using this function is that the user should not
     * modify any fields set except for {@link A0BlockHeader#solution} after this function is
     * called.
     *
     * @param txs list of transactions input to the block (final)
     * @param txTrieRoot the rootHash of the transaction receipt, should correspond with {@code txs}
     * @param stateRoot the root of the world after transactions are executed
     * @param bloom the concatenated blooms of all logs emitted from transactions
     * @param receiptRoot the rootHash of the receipt trie
     * @param energyUsed the amount of energy consumed in the execution of the block
     */
    public void seal(
            List<AionTransaction> txs,
            byte[] txTrieRoot,
            byte[] stateRoot,
            byte[] bloom,
            byte[] receiptRoot,
            long energyUsed) {
        this.getHeader().setTxTrieRoot(txTrieRoot);
        this.getHeader().setStateRoot(stateRoot);
        this.getHeader().setLogsBloom(bloom);
        this.getHeader().setReceiptsRoot(receiptRoot);
        this.getHeader().setEnergyConsumed(energyUsed);

        this.transactionsList = txs;
    }

    @Override
    public String toString() {
        StringBuilder toStringBuff = new StringBuilder();
        parseRLP();

        toStringBuff.setLength(0);
        toStringBuff.append("BlockData [ ");
        toStringBuff.append("hash=").append(ByteUtil.toHexString(this.getHash())).append("\n");
        toStringBuff.append(header.toString());

        if (td != null) {
            toStringBuff.append("  cumulative difficulty=").append(td).append("\n");
        }

        if (mainChain != null) {
            toStringBuff.append("  mainChain=").append(mainChain ? "yes" : "no").append("\n");
        }

        if (!getTransactionsList().isEmpty()) {
            toStringBuff
                    .append("  transactions=")
                    .append(getTransactionsList().size())
                    .append("\n\n");
            toStringBuff.append("  Txs [\n");
            int index = 0;
            for (AionTransaction tx : getTransactionsList()) {
                toStringBuff
                        .append("  ")
                        .append("index=")
                        .append(index++)
                        .append("\n")
                        .append(tx)
                        .append("\n");
            }
            toStringBuff.append("  ]\n");
        } else {
            toStringBuff.append("  Txs []\n");
        }
        toStringBuff.append("]\n");
        toStringBuff.append(Hex.toHexString(this.getEncoded())).append("\n");

        return toStringBuff.toString();
    }

    private byte[] parseTxs(RLPList txTransactions) {

        Trie txsState = new TrieImpl(null);
        for (int i = 0; i < txTransactions.size(); i++) {
            RLPElement transactionRaw = txTransactions.get(i);
            this.transactionsList.add(TxUtil.decode(transactionRaw.getRLPData()));
            txsState.update(RLP.encodeInt(i), transactionRaw.getRLPData());
        }
        return txsState.getRootHash().clone();
    }

    private boolean parseTxs(byte[] expectedRoot, RLPList txTransactions) {

        byte[] txStateRoot = parseTxs(txTransactions);
        String calculatedRoot = Hex.toHexString(txStateRoot);
        if (!calculatedRoot.equals(Hex.toHexString(expectedRoot))) {
            LOG.debug(
                    "Transactions trie root validation failed for block #{}",
                    this.header.getNumber());
            return false;
        }

        return true;
    }

    public boolean isGenesis() {
        return this.header.isGenesis();
    }

    public boolean isEqual(AionBlock block) {
        return Arrays.equals(this.getHash(), block.getHash());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AionBlock block = (AionBlock) o;
        return Arrays.equals(getEncoded(), block.getEncoded());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(rlpEncoded);
    }

    public byte[] getEncoded() {
        if (rlpEncoded == null) {
            byte[] header = this.header.getEncoded();

            List<byte[]> block = getBodyElements();
            block.add(0, header);
            byte[][] elements = block.toArray(new byte[block.size()][]);

            this.rlpEncoded = RLP.encodeList(elements);
        }
        return rlpEncoded;
    }

    public byte[] getEncodedWithoutNonce() {
        parseRLP();
        return this.header.getEncodedWithoutNonce();
    }

    @Override
    public String getShortHash() {
        parseRLP();
        return Hex.toHexString(getHash()).substring(0, 6);
    }

    @Override
    public String getShortDescr() {
        return "#"
                + getNumber()
                + " ("
                + Hex.toHexString(getHash()).substring(0, 6)
                + " <~ "
                + Hex.toHexString(getParentHash()).substring(0, 6)
                + ") Txs:"
                + getTransactionsList().size();
    }

    @Override
    public long getNrgConsumed() {
        parseRLP();
        return this.header.getEnergyConsumed();
    }

    @Override
    public long getNrgLimit() {
        parseRLP();
        return this.header.getEnergyLimit();
    }

    // TODO: [Unity] Either remove this method, or remove the cast on header
    public static AionBlock createBlockFromNetwork(BlockHeader header, byte[] body) {
        if (header == null || body == null) return null;

        AionBlock block = new AionBlock();
        block.header = (A0BlockHeader) header;
        block.parsed = true;

        RLPList items = (RLPList) RLP.decode2(body).get(0);
        RLPList transactions = (RLPList) items.get(0);

        if (!block.parseTxs(header.getTxTrieRoot(), transactions)) {
            return null;
        }

        return block;
    }

    public static AionBlock fromRLP(byte[] rlpEncoded, boolean isUnsafe) {
        RLPList params = RLP.decode2(rlpEncoded);

        // ensuring the expected types list before type casting
        if (params.get(0) instanceof RLPList) {
            RLPList blockRLP = (RLPList) params.get(0);

            if (blockRLP.get(0) instanceof RLPList && blockRLP.get(1) instanceof RLPList) {

                // Parse Header
                RLPList headerRLP = (RLPList) blockRLP.get(0);
                A0BlockHeader header;
                try {
                    header = A0BlockHeader.fromRLP(headerRLP, isUnsafe);
                } catch (Exception e) {
                    return null;
                }
                if (header == null) {
                    return null;
                }

                AionBlock block = new AionBlock();
                block.header = header;
                block.parsed = true;

                // Parse Transactions
                RLPList transactions = (RLPList) blockRLP.get(1);
                if (!block.parseTxs(header.getTxTrieRoot(), transactions)) {
                    return null;
                }

                return block;
            }
        }
        // not an AionBlock encoding
        return null;
    }

    public void setCumulativeDifficulty(BigInteger _td) {
        td = _td;
    }

    /** use for cli tooling */
    private Boolean mainChain;

    public void setMainChain() {
        mainChain = true;
    }
}
