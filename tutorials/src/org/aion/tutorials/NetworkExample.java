package org.aion.tutorials;

import org.aion.api.IAionAPI;
import org.aion.api.type.ApiMsg;
import org.aion.api.type.Node;
import org.aion.api.type.SyncInfo;

import java.util.List;

public class NetworkExample {

    public static void main(String[] args) {

        // connect to Java API
        IAionAPI api = IAionAPI.init();
        ApiMsg apiMsg = api.connect(IAionAPI.LOCALHOST_URL);

        // failed connection
        if (apiMsg.isError()) {
            System.out.format("Could not connect due to <%s>%n", apiMsg.getErrString());
            System.exit(-1);
        }

        // 1. eth_syncing

        // get sync status
        SyncInfo status = api.getNet().syncInfo().getObject();

        // print status
        System.out.format("{ currentBlock: %d,%n  highestBlock: %d,%n  maxImportBlocks: %d }",
                          status.getChainBestBlock(),
                          status.getNetworkBestBlock(),
                          status.getMaxImportBlocks());

        // 2. net_peerCount

        // get sync status
        List<Node> peers = api.getNet().getActiveNodes().getObject();

        // print status
        System.out.format("number of active peers = %d%n", peers.size());

        // 3. net_listening


        // disconnect from api
        api.destroyApi();

        System.exit(0);
    }
}
