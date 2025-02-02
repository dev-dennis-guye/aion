package org.aion.zero.impl.types;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.aion.base.AionTransaction;
import org.aion.mcf.blockchain.Block;
import org.aion.rlp.RLP;

/** Abstract Block class. */
public abstract class AbstractBlock implements Block {

    protected List<AionTransaction> transactionsList = new CopyOnWriteArrayList<>();

    @Override
    public boolean isEqual(Block block) {
        return Arrays.equals(this.getHash(), block.getHash());
    }

    public abstract String getShortDescr();

    public abstract void parseRLP();

    /**
     * check if param block is son of this block
     *
     * @param block - possible a son of this
     * @return - true if this block is parent of param block
     */
    public boolean isParentOf(Block block) {
        return Arrays.equals(this.getHash(), block.getParentHash());
    }

    public byte[] getEncodedBody() {
        List<byte[]> body = getBodyElements();
        byte[][] elements = body.toArray(new byte[body.size()][]);
        return RLP.encodeList(elements);
    }

    public List<byte[]> getBodyElements() {
        parseRLP();
        byte[] transactions = getTransactionsEncoded();
        List<byte[]> body = new ArrayList<>();
        body.add(transactions);
        return body;
    }

    public byte[] getTransactionsEncoded() {

        byte[][] transactionsEncoded = new byte[transactionsList.size()][];
        int i = 0;
        for (AionTransaction tx : transactionsList) {
            transactionsEncoded[i] = tx.getEncoded();
            ++i;
        }
        return RLP.encodeList(transactionsEncoded);
    }
}
