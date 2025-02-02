package org.aion.mcf.blockchain;

import java.math.BigInteger;
import org.aion.types.AionAddress;
import org.json.JSONObject;

/** @author jay */
public interface BlockHeader {

    // Getter
    byte[] getParentHash();

    byte[] getStateRoot();

    byte[] getTxTrieRoot();

    byte[] getReceiptsRoot();

    byte[] getLogsBloom();

    byte[] getExtraData();

    byte[] getHash();

    byte[] getEncoded();

    AionAddress getCoinbase();

    long getTimestamp();

    long getNumber();

    // Setter
    void setCoinbase(AionAddress _cb);

    void setStateRoot(byte[] _strt);

    void setReceiptsRoot(byte[] _rcrt);

    void setTimestamp(long _ts);

    void setNumber(long _nb);

    void setLogsBloom(byte[] _lb);

    void setExtraData(byte[] _ed);

    boolean isGenesis();

    byte[] getDifficulty();

    BigInteger getDifficultyBI();

    void setDifficulty(byte[] _diff);

    long getEnergyConsumed();

    long getEnergyLimit();
    
    byte getVersion();

    byte[] getMineHash();

    byte[] getNonce();

    byte[] getSolution();

    JSONObject toJSON();
}
