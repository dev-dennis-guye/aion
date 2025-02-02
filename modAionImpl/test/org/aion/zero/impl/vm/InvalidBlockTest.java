package org.aion.zero.impl.vm;

import static org.junit.Assert.assertEquals;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import org.aion.avm.core.dappreading.JarBuilder;
import org.aion.avm.userlib.CodeAndArguments;
import org.aion.avm.userlib.abi.ABIDecoder;
import org.aion.avm.userlib.abi.ABIException;
import org.aion.avm.userlib.abi.ABIToken;
import org.aion.base.AionTransaction;
import org.aion.base.TransactionTypes;
import org.aion.crypto.ECKey;
import org.aion.mcf.blockchain.Block;
import org.aion.zero.impl.core.ImportResult;
import org.aion.base.TransactionTypeRule;
import org.aion.types.AionAddress;
import org.aion.vm.avm.LongLivedAvm;
import org.aion.zero.impl.blockchain.StandaloneBlockchain;
import org.aion.zero.impl.types.AionBlock;
import org.aion.zero.impl.types.AionBlockSummary;
import org.aion.zero.impl.vm.contracts.Contract;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class InvalidBlockTest {
    private StandaloneBlockchain blockchain;
    private ECKey deployerKey;
    private long energyPrice = 1;

    @BeforeClass
    public static void setupAvm() {
        LongLivedAvm.createAndStartLongLivedAvm();
        TransactionTypeRule.allowAVMContractTransaction();
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
    public void test() {
        BigInteger nonce =
                this.blockchain
                        .getRepository()
                        .getNonce(new AionAddress(this.deployerKey.getAddress()));
        List<AionTransaction> transactions = makeTransactions(20, nonce);

        Block parent = this.blockchain.getBestBlock();
        AionBlock block = this.blockchain.createNewBlock(parent, transactions, false);

        Pair<ImportResult, AionBlockSummary> res =
                this.blockchain.tryToConnectAndFetchSummary(block);

        // A correct block is produced instead of an invalid one. But the correct block only
        // contains the valid transactions, not all of them.
        assertEquals(ImportResult.IMPORTED_BEST, res.getLeft());
        assertEquals(13, res.getRight().getReceipts().size());
    }

    private List<AionTransaction> makeTransactions(int num, BigInteger initialNonce) {
        List<AionTransaction> transactions = new ArrayList<>();

        byte[] jar =
                new CodeAndArguments(
                                JarBuilder.buildJarForMainAndClasses(Contract.class, ABIDecoder.class, ABIToken.class, ABIException.class),
                                new byte[0])
                        .encodeToBytes();
        BigInteger nonce = initialNonce;

        for (int i = 0; i < num; i++) {

            AionTransaction transaction =
                    AionTransaction.create(
                            deployerKey,
                            nonce.toByteArray(),
                            null,
                            BigInteger.ZERO.toByteArray(),
                            jar,
                            5_000_000L,
                            10_000_000_000L,
                            TransactionTypes.AVM_CREATE_CODE);

            transactions.add(transaction);
            nonce = nonce.add(BigInteger.ONE);
        }

        return transactions;
    }
}
