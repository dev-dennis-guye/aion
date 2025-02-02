package org.aion.precompiled.contracts.ATB;

import static com.google.common.truth.Truth.assertThat;
import static org.aion.precompiled.contracts.ATB.BridgeTestUtils.dummyContext;

import java.util.List;
import org.aion.crypto.HashUtil;
import org.aion.precompiled.ExternalStateForTests;
import org.aion.precompiled.type.IExternalStateForPrecompiled;
import org.aion.precompiled.type.PrecompiledTransactionContext;
import org.aion.types.AionAddress;
import org.aion.types.InternalTransaction;
import org.aion.types.Log;
import org.aion.util.bytes.ByteUtil;
import org.junit.Before;
import org.junit.Test;

public class BridgeControllerOwnerTest {

    private BridgeStorageConnector connector;
    private BridgeController controller;
    private List<Log> logs;
    private List<InternalTransaction> internalTransactions;
    private List<AionAddress> deletedAddresses;

    private static final AionAddress CONTRACT_ADDR =
            new AionAddress(HashUtil.h256("contractAddress".getBytes()));
    private static final AionAddress OWNER_ADDR =
            new AionAddress(HashUtil.h256("ownerAddress".getBytes()));

    @Before
    public void beforeEach() {
        IExternalStateForPrecompiled worldState = ExternalStateForTests.usingDefaultRepository();
        this.connector = new BridgeStorageConnector(worldState, CONTRACT_ADDR);

        PrecompiledTransactionContext context = dummyContext();
        this.logs = context.getLogs();
        this.internalTransactions = context.getInternalTransactions();
        this.deletedAddresses = context.getDeletedAddresses();
        this.controller = new BridgeController(connector, this.logs, CONTRACT_ADDR, OWNER_ADDR);
    }

    @Test
    public void testInitialize() {
        this.controller.initialize();
        assertThat(this.connector.getOwner()).isEqualTo(OWNER_ADDR.toByteArray());
    }

    @Test
    public void testTransferOwnership() {
        byte[] transferOwnership = HashUtil.keccak256("ChangedOwner(address)".getBytes());
        byte[] newOwner = HashUtil.h256("newOwner".getBytes());
        this.controller.initialize();
        this.controller.setNewOwner(OWNER_ADDR.toByteArray(), newOwner);

        // sanity check
        assertThat(this.connector.getNewOwner()).isEqualTo(newOwner);
        ErrCode err = this.controller.acceptOwnership(newOwner);
        assertThat(err).isEqualTo(ErrCode.NO_ERROR);

        assertThat(this.connector.getOwner()).isEqualTo(newOwner);
        // check that an event was properly generated
        assertThat(this.logs.size()).isEqualTo(1);

        Log changedOwnerLog = this.logs.get(0);
        assertThat(changedOwnerLog.copyOfData()).isEqualTo(ByteUtil.EMPTY_BYTE_ARRAY);
        assertThat(changedOwnerLog.copyOfTopics().get(0)).isEqualTo(transferOwnership);
        assertThat(changedOwnerLog.copyOfTopics().get(1)).isEqualTo(newOwner);
    }

    @Test
    public void testInvalidOwnerTransferOwnership() {
        byte[] notOwner = HashUtil.h256("not owner".getBytes());
        byte[] newOwner = HashUtil.h256("newOwner".getBytes());
        this.controller.initialize();
        ErrCode err = this.controller.setNewOwner(notOwner, newOwner);
        assertThat(err).isEqualTo(ErrCode.NOT_OWNER);
    }
}
