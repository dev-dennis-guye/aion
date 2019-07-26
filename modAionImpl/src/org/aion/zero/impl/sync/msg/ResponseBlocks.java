package org.aion.zero.impl.sync.msg;

import static org.aion.mcf.types.AbstractBlockHeader.RLP_BH_SEALTYPE;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.aion.mcf.blockchain.Block;
import org.aion.mcf.types.AbstractBlockHeader.BlockSealType;
import org.aion.p2p.Ctrl;
import org.aion.p2p.Msg;
import org.aion.p2p.Ver;
import org.aion.rlp.RLP;
import org.aion.rlp.RLPElement;
import org.aion.rlp.RLPList;
import org.aion.zero.impl.sync.Act;
import org.aion.zero.impl.types.AionBlock;
import org.aion.zero.impl.types.StakingBlock;
import org.aion.zero.types.A0BlockHeader;
import org.aion.zero.types.StakedBlockHeader;

/**
 * Response message to a request for a block range.
 *
 * @author Alexandra Roatis
 */
public final class ResponseBlocks extends Msg {

    private final List<Block> blocks;

    /**
     * Constructor for block range responses.
     *
     * @param blocks a list of blocks representing the response to a requested range
     * @implNote The given blocks are purposefully not deep copied to minimize resource
     *     instantiation. This is a reasonable choice given that these objects are created to be
     *     encoded and transmitted over the network and therefore there is the expectation that they
     *     will not be utilized further.
     */
    public ResponseBlocks(final List<Block> blocks) {
        super(Ver.V1, Ctrl.SYNC, Act.RESPONSE_BLOCKS);

        // ensure input is not null
        Objects.requireNonNull(blocks);

        this.blocks = blocks;
    }

    /**
     * Decodes a message into a block range response.
     *
     * @param message a {@code byte} array representing a response to a block range request.
     * @return the decoded block range response
     * @implNote The decoder returns {@code null} when given an empty message.
     */
    public static ResponseBlocks decode(final byte[] message) {
        if (message == null || message.length == 0) {
            return null;
        } else {
            RLPList list = RLP.decode2(message);
            if (list.get(0) instanceof RLPList) {
                list = (RLPList) list.get(0);
            } else {
                return null;
            }

            List<Block> blocks = new ArrayList<>();
            Block current = null;
            for (RLPElement encoded : list) {
                try { // preventative try-catch: it's unlikely that exceptions can pass up to here

                    RLPList params = RLP.decode2(encoded.getRLPData());
                    RLPList blockRLP = (RLPList) params.get(0);
                    if (blockRLP.get(0) instanceof RLPList && blockRLP.get(1) instanceof RLPList) {
                        // Parse Header
                        RLPList headerRLP = (RLPList) blockRLP.get(0);

                        byte[] type = headerRLP.get(RLP_BH_SEALTYPE).getRLPData();
                        if (type[0] == BlockSealType.SEAL_POW_BLOCK.ordinal()) {
                            current = AionBlock.fromRLPList(params, true);
                        } else if (type[0] == BlockSealType.SEAL_POS_BLOCK.ordinal()) {
                            current = StakingBlock.fromRLPList(params, true);
                        }
                    }
                } catch (Exception e) {
                    return null;
                }
                if (current == null) {
                    return null;
                } else {
                    blocks.add(current);
                }
            }
            return new ResponseBlocks(blocks);
        }
    }

    @Override
    public byte[] encode() {
        byte[][] toEncode = new byte[this.blocks.size()][];

        int i = 0;
        for (Block block : blocks) {
            toEncode[i] = block.getEncoded();
            i++;
        }

        return RLP.encodeList(toEncode);
    }

    /**
     * Returns the list of blocks representing the response to a requested block range.
     *
     * @return the list of blocks representing the response to a requested block range
     */
    public List<Block> getBlocks() {
        return blocks;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ResponseBlocks that = (ResponseBlocks) o;
        return Objects.equals(blocks, that.blocks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(blocks);
    }
}
