package org.aion.mcf.blockchain;

import org.aion.base.AionTransaction;
import org.aion.log.AionLoggerFactory;
import org.aion.log.LogEnum;
import org.aion.mcf.db.IBlockStoreBase;
import org.aion.mcf.db.RepositoryCache;
import org.aion.mcf.types.AbstractTxReceipt;
import org.slf4j.Logger;

/** Transaction executor base class. */
public abstract class TxExecutorBase<
        BLK extends Block, BS extends IBlockStoreBase, TR extends AbstractTxReceipt> {

    protected static final Logger LOG = AionLoggerFactory.getLogger(LogEnum.VM.toString());

    protected AionTransaction tx;

    protected RepositoryCache<?, ?> track;

    protected RepositoryCache<?, ?> cacheTrack;

    protected BS blockStore;

    protected TR receipt;

    protected BLK currentBlock;
}
