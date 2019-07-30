package org.aion.api.server.rpc2;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import org.aion.api.server.rpc2.autogen.Rpc;
import org.aion.util.types.ByteArrayWrapper;
import org.aion.zero.impl.blockchain.AionImpl;
import org.aion.zero.impl.blockchain.IAionChain;
import org.aion.zero.impl.types.StakingBlock;
import org.aion.zero.types.StakedBlockHeader;
import org.apache.commons.collections4.map.LRUMap;

public class RpcImpl implements Rpc {

    private IAionChain ac;
    //TODO : [unity] find the proper number for chaching the template.
    private Map<ByteArrayWrapper, StakingBlock> stakingBlockTemplate = Collections
        .synchronizedMap(new LRUMap<>(64));
    private ReentrantLock blockTemplateLock;

    RpcImpl(final IAionChain _ac) {
        if (_ac == null) {
            throw  new NullPointerException();
        }

        ac = _ac;
        blockTemplateLock = new ReentrantLock();
    }

    @Override
    public byte[] getseed() {
        return ac.getBlockchain().getSeed();
    }

    @Override
    public byte[] submitseed(byte[] newSeed, byte[] pubKey) throws Exception {
        if (newSeed == null || pubKey == null) {
            throw new NullPointerException();
        }

        if (newSeed.length != StakedBlockHeader.SEED_LENGTH
                || pubKey.length != StakedBlockHeader.PUBKEY_LENGTH) {
            throw new IllegalArgumentException("Invalid arguments length");
        }

        blockTemplateLock.lock();
        try {
            StakingBlock template =
                    (StakingBlock)
                            ac.getBlockchain()
                                    .createStakingBlockTemplate(
                                            ac.getAionHub()
                                                    .getPendingState()
                                                    .getPendingTransactions(),
                                            pubKey,
                                            newSeed);

            if (template == null) {
                throw new Exception("GetStakingBlockTemplate failed!");
            }

            byte[] sealhash = template.getHeader().getMineHash();
            stakingBlockTemplate.put(ByteArrayWrapper.wrap(sealhash), template);

            return sealhash;
        } finally {
            blockTemplateLock.unlock();
        }
    }

    @Override
    public boolean submitsignature(byte[] signature, byte[] sealhash) {
        if (signature == null || sealhash == null) {
            throw new NullPointerException();
        }

        if (signature.length != StakedBlockHeader.SIG_LENGTH
            || sealhash.length != 32) {
            throw new IllegalArgumentException("Invalid arguments length");
        }

        StakingBlock block = stakingBlockTemplate.get(ByteArrayWrapper.wrap(sealhash));
        if (block == null) {
            return false;
        }

        block.getHeader().setSignature(signature);

        return AionImpl.inst().addNewMinedBlock(block).isBest();
    }
}
