package org.aion.api.server.rpc2;

import java.util.concurrent.locks.ReentrantLock;
import org.aion.api.server.rpc2.autogen.Rpc;
import org.aion.zero.impl.blockchain.IAionChain;
import org.aion.zero.impl.types.StakingBlock;
import org.aion.zero.types.StakedBlockHeader;

public class RpcImpl implements Rpc {

    private IAionChain ac;
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
        if (! ac.getAionHub().getBlockchain().isUnityForkEnabled()) {
            throw new IllegalStateException("UnityForkNotEnabled!");
        }

        return ac.getBlockchain().getSeed();
    }

    @Override
    public byte[] submitseed(byte[] newSeed, byte[] pubKey) throws Exception {
        if (! ac.getAionHub().getBlockchain().isUnityForkEnabled()) {
            throw new IllegalStateException("UnityForkNotEnabled!");
        }

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
                            ac.getAionHub().getPendingState().getPendingTransactions()
                            , pubKey
                            , newSeed);

            if (template == null) {
                throw new Exception("GetStakingBlockTemplate failed!");
            }

            return template.getHeader().getMineHash();
        } finally {
            blockTemplateLock.unlock();
        }
    }

    @Override
    public boolean submitsignature(byte[] signature, byte[] sealhash) {
        if (! ac.getAionHub().getBlockchain().isUnityForkEnabled()) {
            throw new IllegalStateException("UnityForkNotEnabled!");
        }

        if (signature == null || sealhash == null) {
            throw new NullPointerException();
        }

        if (signature.length != StakedBlockHeader.SIG_LENGTH
            || sealhash.length != 32) {
            throw new IllegalArgumentException("Invalid arguments length");
        }

        StakingBlock block = (StakingBlock) ac.getBlockchain().getCachingStakingBlockTemplate(sealhash);
        if (block == null) {
            return false;
        }

        block.getHeader().setSignature(signature);
        ac.getBlockchain().putSealedNewStakingBlock(block);

        return true;
    }
}
