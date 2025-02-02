package org.aion.zero.impl.vm;

import static com.google.common.truth.Truth.assertThat;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.aion.avm.core.dappreading.JarBuilder;
import org.aion.avm.userlib.CodeAndArguments;
import org.aion.avm.userlib.abi.ABIEncoder;
import org.aion.base.AionTransaction;
import org.aion.base.TransactionTypes;
import org.aion.base.TxUtil;
import org.aion.crypto.AddressSpecs;
import org.aion.crypto.ECKey;
import org.aion.zero.impl.core.ImportResult;
import org.aion.base.TransactionTypeRule;
import org.aion.types.AionAddress;
import org.aion.vm.avm.LongLivedAvm;
import org.aion.zero.impl.blockchain.StandaloneBlockchain;
import org.aion.zero.impl.types.AionBlock;
import org.aion.zero.impl.types.AionBlockSummary;
import org.aion.zero.impl.vm.contracts.AvmHelloWorld;
import org.aion.base.AionTxReceipt;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class AvmHelloWorldTest {
    private StandaloneBlockchain blockchain;
    private ECKey deployerKey;

    @BeforeClass
    public static void setupAvm() {
        LongLivedAvm.createAndStartLongLivedAvm();
    }

    @AfterClass
    public static void tearDownAvm() {
        LongLivedAvm.destroy();
        TransactionTypeRule.disallowAVMContractTransaction();
    }

    @Before
    public void setup() {
        StandaloneBlockchain.Bundle bundle =
                new StandaloneBlockchain.Builder()
                        .withDefaultAccounts()
                        .withValidatorConfiguration("simple")
                        .withAvmEnabled()
                        .build();
        this.blockchain = bundle.bc;
        this.deployerKey = bundle.privateKeys.get(0);
    }

    @After
    public void tearDown() {
        this.blockchain = null;
        this.deployerKey = null;
    }

    @Test
    public void testDeployContract() {
        TransactionTypeRule.allowAVMContractTransaction();
        byte[] jar = getJarBytes();
        AionTransaction transaction =
                AionTransaction.create(
                        deployerKey,
                        new byte[0],
                        null,
                        new byte[0],
                        jar,
                        5_000_000,
                        1,
                        TransactionTypes.AVM_CREATE_CODE);

        AionBlock block =
                this.blockchain.createNewBlock(
                        this.blockchain.getBestBlock(),
                        Collections.singletonList(transaction),
                        false);
        Pair<ImportResult, AionBlockSummary> connectResult =
                this.blockchain.tryToConnectAndFetchSummary(block);
        AionTxReceipt receipt = connectResult.getRight().getReceipts().get(0);

        // Check the block was imported, the contract has the Avm prefix, and deployment succeeded.
        assertThat(connectResult.getLeft()).isEqualTo(ImportResult.IMPORTED_BEST);
        assertThat(receipt.getTransactionOutput()[0]).isEqualTo(AddressSpecs.A0_IDENTIFIER);
        assertThat(receipt.isSuccessful()).isTrue();

        // verify that the output is indeed the contract address
        AionAddress contractAddress = TxUtil.calculateContractAddress(transaction);
        assertThat(contractAddress.toByteArray()).isEqualTo(receipt.getTransactionOutput());
    }

    @Test
    public void testDeployAndCallContract() {
        TransactionTypeRule.allowAVMContractTransaction();
        // Deploy the contract.
        byte[] jar = getJarBytes();
        AionTransaction transaction =
                AionTransaction.create(
                        deployerKey,
                        new byte[0],
                        null,
                        new byte[0],
                        jar,
                        5_000_000,
                        1,
                        TransactionTypes.AVM_CREATE_CODE);

        AionBlock block =
                this.blockchain.createNewBlock(
                        this.blockchain.getBestBlock(),
                        Collections.singletonList(transaction),
                        false);
        Pair<ImportResult, AionBlockSummary> connectResult =
                this.blockchain.tryToConnectAndFetchSummary(block);
        AionTxReceipt receipt = connectResult.getRight().getReceipts().get(0);

        // Check the block was imported, the contract has the Avm prefix, and deployment succeeded.
        assertThat(connectResult.getLeft()).isEqualTo(ImportResult.IMPORTED_BEST);
        assertThat(receipt.getTransactionOutput()[0]).isEqualTo(AddressSpecs.A0_IDENTIFIER);
        assertThat(receipt.isSuccessful()).isTrue();

        AionAddress contract = new AionAddress(receipt.getTransactionOutput());
        // verify that the output is indeed the contract address
        assertThat(TxUtil.calculateContractAddress(transaction)).isEqualTo(contract);
        byte[] call = getCallArguments();
        transaction =
                AionTransaction.create(
                        deployerKey,
                        BigInteger.ONE.toByteArray(),
                        contract,
                        new byte[0],
                        call,
                        2_000_000,
                        1,
                        TransactionTypes.DEFAULT);

        block =
                this.blockchain.createNewBlock(
                        this.blockchain.getBestBlock(),
                        Collections.singletonList(transaction),
                        false);
        connectResult = this.blockchain.tryToConnectAndFetchSummary(block);
        receipt = connectResult.getRight().getReceipts().get(0);

        // Check the block was imported and the transaction was successful.
        assertThat(connectResult.getLeft()).isEqualTo(ImportResult.IMPORTED_BEST);
        assertThat(receipt.isSuccessful()).isTrue();
    }

    @Test
    public void testDeployAndCallContractInTheSameBlock() {
        TransactionTypeRule.allowAVMContractTransaction();
        // Deploy the contract.
        byte[] jar = getJarBytes();
        AionTransaction transaction =
                AionTransaction.create(
                        deployerKey,
                        new byte[0],
                        null,
                        new byte[0],
                        jar,
                        5_000_000,
                        1,
                        TransactionTypes.AVM_CREATE_CODE);

        List<AionTransaction> ls = new ArrayList<>();
        ls.add(transaction);

        byte[] call = getCallArguments();
        AionTransaction transaction2 =
                AionTransaction.create(
                        deployerKey,
                        BigInteger.ONE.toByteArray(),
                        TxUtil.calculateContractAddress(transaction),
                        new byte[0],
                        call,
                        2_000_000,
                        1,
                        TransactionTypes.DEFAULT);

        ls.add(transaction2);

        AionBlock block = this.blockchain.createNewBlock(this.blockchain.getBestBlock(), ls, false);
        Pair<ImportResult, AionBlockSummary> connectResult =
                this.blockchain.tryToConnectAndFetchSummary(block);

        assertThat(connectResult.getLeft()).isEqualTo(ImportResult.IMPORTED_BEST);
        assertThat(connectResult.getRight().getReceipts().size() == 2);

        // Check the block was imported, the contract has the Avm prefix, and deployment succeeded.
        AionTxReceipt receipt = connectResult.getRight().getReceipts().get(0);
        assertThat(receipt.getTransactionOutput()[0]).isEqualTo(AddressSpecs.A0_IDENTIFIER);
        assertThat(receipt.isSuccessful()).isTrue();

        // Check the call success
        receipt = connectResult.getRight().getReceipts().get(1);
        assertThat(receipt.isSuccessful()).isTrue();
    }

    private byte[] getCallArguments() {
        return ABIEncoder.encodeOneString("sayHello");
    }

    private byte[] getJarBytes() {
        return new CodeAndArguments(
                        JarBuilder.buildJarForMainAndClassesAndUserlib(AvmHelloWorld.class),
                        new byte[0])
                .encodeToBytes();
    }
}
