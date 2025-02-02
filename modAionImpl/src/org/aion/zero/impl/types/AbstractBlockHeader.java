package org.aion.zero.impl.types;

import java.math.BigInteger;
import org.aion.log.AionLoggerFactory;
import org.aion.mcf.blockchain.BlockHeader;
import org.aion.types.AionAddress;

/** Abstract BlockHeader. */
public abstract class AbstractBlockHeader implements BlockHeader {

    public static final int NONCE_LENGTH = 32;
    public static final int SOLUTIONSIZE = 1408;
    private static final int MAX_DIFFICULTY_LENGTH = 16;

    protected byte version;

    /* The SHA3 256-bit hash of the parent block, in its entirety */
    protected byte[] parentHash;

    /*
     * The 256-bit address to which all fees collected from the successful
     * mining of this block be transferred; formally
     */
    protected AionAddress coinbase;
    /*
     * The SHA3 256-bit hash of the root node of the state trie, after all
     * transactions are executed and finalisations applied
     */
    protected byte[] stateRoot;
    /*
     * The SHA3 256-bit hash of the root node of the trie structure populated
     * with each transaction in the transaction list portion, the trie is
     * populate by [key, val] --> [rlp(index), rlp(tx_recipe)] of the block
     */
    protected byte[] txTrieRoot;
    /*
     * The SHA3 256-bit hash of the root node of the trie structure populated
     * with each transaction recipe in the transaction recipes list portion, the
     * trie is populate by [key, val] --> [rlp(index), rlp(tx_recipe)] of the
     * block
     */
    protected byte[] receiptTrieRoot;

    /* todo: comment it when you know what the fuck it is */
    protected byte[] logsBloom;
    /*
     * A scalar value corresponding to the difficulty level of this block. This
     * can be calculated from the previous block’s difficulty level and the
     * timestamp
     */
    protected byte[] difficulty;

    /*
     * A scalar value equal to the reasonable output of Unix's time() at this
     * block's inception
     */
    protected long timestamp;

    /*
     * A scalar value equal to the number of ancestor blocks. The genesis block
     * has a number of zero
     */
    protected long number;

    /*
     * An arbitrary byte array containing data relevant to this block. With the
     * exception of the genesis block, this must be 32 bytes or fewer
     */
    protected byte[] extraData;

    /*
     * A long value containing energy consumed within this block
     */
    protected long energyConsumed;

    /*
     * A long value containing energy limit of this block
     */
    protected long energyLimit;

    public AbstractBlockHeader() {}

    public byte[] getParentHash() {
        return parentHash;
    }

    public AionAddress getCoinbase() {
        return coinbase;
    }

    public void setCoinbase(AionAddress coinbase) {
        this.coinbase = coinbase;
    }

    public byte[] getStateRoot() {
        return this.stateRoot;
    }

    public void setStateRoot(byte[] stateRoot) {
        this.stateRoot = stateRoot;
    }

    public byte[] getTxTrieRoot() {
        return txTrieRoot;
    }

    public void setTxTrieRoot(byte[] txTrieRoot) {
        this.txTrieRoot = txTrieRoot;
    }

    public void setReceiptsRoot(byte[] receiptTrieRoot) {
        this.receiptTrieRoot = receiptTrieRoot;
    }

    public byte[] getReceiptsRoot() {
        return receiptTrieRoot;
    }

    public void setTransactionsRoot(byte[] stateRoot) {
        this.txTrieRoot = stateRoot;
    }

    public byte[] getLogsBloom() {
        return logsBloom;
    }

    public byte[] getDifficulty() {
        return difficulty;
    }

    /**
     * @implNote when the difficulty data field exceed the system limit(16 bytes), this method will
     *     return BigInteger.ZERO for the letting the validate() in the AionDifficultyRule return
     *     false. The difficulty in the PoW blockchain should be always a positive value.
     * @see org.aion.zero.impl.valid.AionDifficultyRule.validate;
     * @return the difficulty as the BigInteger format.
     */
    @SuppressWarnings("JavadocReference")
    public BigInteger getDifficultyBI() {
        if (difficulty == null || difficulty.length > MAX_DIFFICULTY_LENGTH) {
            AionLoggerFactory.getLogger("CONS").error("Invalid difficulty length!");
            return BigInteger.ZERO;
        }
        return new BigInteger(1, difficulty);
    }

    public void setDifficulty(byte[] difficulty) {
        this.difficulty = difficulty;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getNumber() {
        return number;
    }

    public void setNumber(long number) {
        this.number = number;
    }

    public byte[] getExtraData() {
        return extraData;
    }

    public void setLogsBloom(byte[] logsBloom) {
        this.logsBloom = logsBloom;
    }

    public void setExtraData(byte[] extraData) {
        this.extraData = extraData;
    }

    public boolean isGenesis() {
        return this.number == 0;
    }

    public byte getVersion() {
        return this.version;
    }

    public void setVersion(byte version) {
        this.version = version;
    }

    public long getEnergyConsumed() {
        return this.energyConsumed;
    }

    public long getEnergyLimit() {
        return this.energyLimit;
    }

    /**
     * Set the energyConsumed field in header, this is used during block creation
     *
     * @param energyConsumed total energyConsumed during execution of transactions
     */
    public void setEnergyConsumed(long energyConsumed) {
        this.energyConsumed = energyConsumed;
    }
}
