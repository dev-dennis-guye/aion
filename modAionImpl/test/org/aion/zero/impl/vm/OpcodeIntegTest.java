/*
 * Copyright (c) 2017-2018 Aion foundation.
 *
 *     This file is part of the aion network project.
 *
 *     The aion network project is free software: you can redistribute it
 *     and/or modify it under the terms of the GNU General Public License
 *     as published by the Free Software Foundation, either version 3 of
 *     the License, or any later version.
 *
 *     The aion network project is distributed in the hope that it will
 *     be useful, but WITHOUT ANY WARRANTY; without even the implied
 *     warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *     See the GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with the aion network project source files.
 *     If not, see <https://www.gnu.org/licenses/>.
 *
 * Contributors:
 *     Aion foundation.
 */
package org.aion.zero.impl.vm;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.aion.base.AionTransaction;
import org.aion.base.TransactionTypes;
import org.aion.base.TxUtil;
import org.aion.crypto.ECKey;
import org.aion.log.AionLoggerFactory;
import org.aion.log.LogEnum;
import org.aion.mcf.blockchain.Block;
import org.aion.mcf.db.RepositoryCache;
import org.aion.util.types.DataWord;
import org.aion.types.AionAddress;
import org.aion.types.InternalTransaction;
import org.aion.types.Log;
import org.aion.util.bytes.ByteUtil;
import org.aion.util.conversions.Hex;
import org.aion.vm.common.BlockCachingContext;
import org.aion.vm.common.BulkExecutor;
import org.aion.vm.avm.LongLivedAvm;
import org.aion.vm.exception.VMException;
import org.aion.zero.impl.types.BlockContext;
import org.aion.zero.impl.blockchain.StandaloneBlockchain;
import org.aion.zero.impl.blockchain.StandaloneBlockchain.Builder;
import org.aion.zero.impl.vm.contracts.ContractUtils;
import org.aion.base.AionTxExecSummary;
import org.apache.commons.lang3.RandomUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

public class OpcodeIntegTest {
    private static final Logger LOGGER_VM = AionLoggerFactory.getLogger(LogEnum.VM.toString());
    private StandaloneBlockchain blockchain;
    private ECKey deployerKey;
    private AionAddress deployer;
    private BigInteger deployerBalance;

    @Before
    public void setup() {
        StandaloneBlockchain.Bundle bundle =
                (new StandaloneBlockchain.Builder())
                        .withValidatorConfiguration("simple")
                        .withDefaultAccounts()
                        .build();
        blockchain = bundle.bc;
        deployerKey = bundle.privateKeys.get(0);
        deployer = new AionAddress(deployerKey.getAddress());
        deployerBalance = Builder.DEFAULT_BALANCE;

        LongLivedAvm.createAndStartLongLivedAvm();
    }

    @After
    public void tearDown() {
        blockchain = null;
        deployerKey = null;
        deployer = null;
        deployerBalance = null;
        LongLivedAvm.destroy();
    }

    // ====================== test repo & track flushing over multiple levels ======================

    @Test
    public void testNoRevert() throws IOException, VMException {
        RepositoryCache repo = blockchain.getRepository().startTracking();
        AionAddress D = deployContract(repo, "F", "F.sol", BigInteger.ZERO);
        long nrg = 1_000_000;
        long nrgPrice = 1;
        BigInteger nonce = BigInteger.ONE;

        byte[] input = ByteUtil.merge(Hex.decode("f854bb89"), new DataWord(6).getData());
        AionTransaction tx =
                AionTransaction.create(
                        deployerKey,
                        nonce.toByteArray(),
                        D,
                        BigInteger.ZERO.toByteArray(),
                        input,
                        nrg,
                        nrgPrice,
                        TransactionTypes.DEFAULT);

        BlockContext context =
                blockchain.createNewBlockContext(
                        blockchain.getBestBlock(), Collections.singletonList(tx), false);
        AionTxExecSummary summary = executeTransaction(tx, context.block, repo);

        assertEquals("", summary.getReceipt().getError());
        assertEquals(summary.getNrgUsed().longValue(), summary.getNrgUsed().longValue());

        // Check that the logs from our internal transactions are as we expect.
        List<Log> logs = summary.getReceipt().getLogInfoList();
        assertEquals(12, logs.size());
        assertArrayEquals(new DataWord(0).getData(), logs.get(0).copyOfData());
        assertArrayEquals(new DataWord(6).getData(), logs.get(1).copyOfData());
        assertArrayEquals(new DataWord(5).getData(), logs.get(2).copyOfData());
        assertArrayEquals(new DataWord(4).getData(), logs.get(3).copyOfData());
        assertArrayEquals(new DataWord(3).getData(), logs.get(4).copyOfData());
        assertArrayEquals(new DataWord(2).getData(), logs.get(5).copyOfData());
        assertArrayEquals(new DataWord(1).getData(), logs.get(6).copyOfData());
        assertArrayEquals(new DataWord(1).getData(), logs.get(7).copyOfData());
        assertArrayEquals(new DataWord(1).getData(), logs.get(8).copyOfData());
        assertArrayEquals(new DataWord(1).getData(), logs.get(9).copyOfData());
        assertArrayEquals(new DataWord(1).getData(), logs.get(10).copyOfData());
        assertArrayEquals(new DataWord(1).getData(), logs.get(11).copyOfData());
    }

    @Test
    public void testRevertAtBottomLevel() throws IOException, VMException {
        RepositoryCache repo = blockchain.getRepository().startTracking();
        AionAddress D = deployContract(repo, "F", "F.sol", BigInteger.ZERO);
        long nrg = 1_000_000;
        long nrgPrice = 1;
        BigInteger nonce = BigInteger.ONE;

        byte[] input = ByteUtil.merge(Hex.decode("8256cff3"), new DataWord(5).getData());
        AionTransaction tx =
                AionTransaction.create(
                        deployerKey,
                        nonce.toByteArray(),
                        D,
                        BigInteger.ZERO.toByteArray(),
                        input,
                        nrg,
                        nrgPrice,
                        TransactionTypes.DEFAULT);

        BlockContext context =
                blockchain.createNewBlockContext(
                        blockchain.getBestBlock(), Collections.singletonList(tx), false);
        AionTxExecSummary summary = executeTransaction(tx, context.block, repo);

        assertEquals("", summary.getReceipt().getError());
        assertEquals(summary.getNrgUsed().longValue(), summary.getNrgUsed().longValue());

        // Check that the logs from our internal transactions are as we expect.
        List<Log> logs = summary.getReceipt().getLogInfoList();
        assertEquals(8, logs.size());
        assertArrayEquals(new DataWord(0).getData(), logs.get(0).copyOfData());
        assertArrayEquals(new DataWord(5).getData(), logs.get(1).copyOfData());
        assertArrayEquals(new DataWord(4).getData(), logs.get(2).copyOfData());
        assertArrayEquals(new DataWord(3).getData(), logs.get(3).copyOfData());
        assertArrayEquals(new DataWord(2).getData(), logs.get(4).copyOfData());
        assertArrayEquals(new DataWord(2).getData(), logs.get(5).copyOfData());
        assertArrayEquals(new DataWord(2).getData(), logs.get(6).copyOfData());
        assertArrayEquals(new DataWord(2).getData(), logs.get(7).copyOfData());
    }

    @Test
    public void testRevertAtMidLevel() throws IOException, VMException {
        RepositoryCache repo = blockchain.getRepository().startTracking();
        AionAddress D = deployContract(repo, "F", "F.sol", BigInteger.ZERO);
        long nrg = 1_000_000;
        long nrgPrice = 1;
        BigInteger nonce = BigInteger.ONE;

        byte[] input = ByteUtil.merge(Hex.decode("10462fd0"), new DataWord(7).getData());
        AionTransaction tx =
                AionTransaction.create(
                        deployerKey,
                        nonce.toByteArray(),
                        D,
                        BigInteger.ZERO.toByteArray(),
                        input,
                        nrg,
                        nrgPrice,
                        TransactionTypes.DEFAULT);

        BlockContext context =
                blockchain.createNewBlockContext(
                        blockchain.getBestBlock(), Collections.singletonList(tx), false);
        AionTxExecSummary summary = executeTransaction(tx, context.block, repo);

        assertEquals("", summary.getReceipt().getError());
        assertEquals(summary.getNrgUsed().longValue(), summary.getNrgUsed().longValue());

        // Check that the logs from our internal transactions are as we expect.
        List<Log> logs = summary.getReceipt().getLogInfoList();
        assertEquals(8, logs.size());
        assertArrayEquals(new DataWord(0).getData(), logs.get(0).copyOfData());
        assertArrayEquals(new DataWord(7).getData(), logs.get(1).copyOfData());
        assertArrayEquals(new DataWord(6).getData(), logs.get(2).copyOfData());
        assertArrayEquals(new DataWord(5).getData(), logs.get(3).copyOfData());
        assertArrayEquals(new DataWord(4).getData(), logs.get(4).copyOfData());
        assertArrayEquals(new DataWord(4).getData(), logs.get(5).copyOfData());
        assertArrayEquals(new DataWord(4).getData(), logs.get(6).copyOfData());
        assertArrayEquals(new DataWord(4).getData(), logs.get(7).copyOfData());
    }

    // ======================================= test CALLCODE =======================================

    @Test
    public void testCallcodeStorage() throws IOException, VMException {
        RepositoryCache repo = blockchain.getRepository().startTracking();
        BigInteger n = new BigInteger("7638523");
        AionAddress D = deployContract(repo, "D", "D.sol", BigInteger.ZERO);
        AionAddress E = deployContract(repo, "E", "D.sol", BigInteger.ZERO);

        // Deployer calls contract D which performs CALLCODE to call contract E. We expect that the
        // storage in contract D is modified by the code that is called in contract E.
        long nrg = 1_000_000;
        long nrgPrice = 1;
        BigInteger nonce = BigInteger.TWO;
        byte[] input =
                ByteUtil.merge(Hex.decode("5cce9fc2"), E.toByteArray()); // use CALLCODE on E.
        input = ByteUtil.merge(input, new DataWord(n).getData()); // pass in 'n' also.

        AionTransaction tx =
                AionTransaction.create(
                        deployerKey,
                        nonce.toByteArray(),
                        D,
                        BigInteger.ZERO.toByteArray(),
                        input,
                        nrg,
                        nrgPrice,
                        TransactionTypes.DEFAULT);
        assertEquals(deployer, tx.getSenderAddress());
        assertEquals(D, tx.getDestinationAddress());

        BlockContext context =
                blockchain.createNewBlockContext(
                        blockchain.getBestBlock(), Collections.singletonList(tx), false);
        AionTxExecSummary summary = executeTransaction(tx, context.block, repo);
        assertEquals("", summary.getReceipt().getError());
        assertEquals(summary.getNrgUsed().longValue(), summary.getNrgUsed().longValue());
        nonce = nonce.add(BigInteger.ONE);

        // When we call into contract D we should find its storage is modified so that 'n' is set.
        input = Hex.decode("3e955225");
        tx =
                AionTransaction.create(
                        deployerKey,
                        nonce.toByteArray(),
                        D,
                        BigInteger.ZERO.toByteArray(),
                        input,
                        nrg,
                        nrgPrice,
                        TransactionTypes.DEFAULT);
        assertEquals(deployer, tx.getSenderAddress());
        assertEquals(D, tx.getDestinationAddress());

        context =
                blockchain.createNewBlockContext(
                        blockchain.getBestBlock(), Collections.singletonList(tx), false);
        BigInteger inStore =
                new BigInteger(executeTransaction(tx, context.block, repo).getResult());
        System.err.println("Found in D's storage for n: " + inStore);
        assertEquals(n, inStore);
        nonce = nonce.add(BigInteger.ONE);

        // When we call into contract E we should find its storage is unmodified.
        tx =
                AionTransaction.create(
                        deployerKey,
                        nonce.toByteArray(),
                        E,
                        BigInteger.ZERO.toByteArray(),
                        input,
                        nrg,
                        nrgPrice,
                        TransactionTypes.DEFAULT);
        assertEquals(deployer, tx.getSenderAddress());
        assertEquals(E, tx.getDestinationAddress());

        context =
                blockchain.createNewBlockContext(
                        blockchain.getBestBlock(), Collections.singletonList(tx), false);
        inStore = new BigInteger(executeTransaction(tx, context.block, repo).getResult());
        assertEquals(BigInteger.ZERO, inStore);
    }

    @Test
    public void testCallcodeActors() throws IOException, VMException {
        RepositoryCache repo = blockchain.getRepository().startTracking();
        AionAddress D = deployContract(repo, "D", "D.sol", BigInteger.ZERO);
        AionAddress E = deployContract(repo, "E", "D.sol", BigInteger.ZERO);

        // Deployer calls contract D which performs CALLCODE to call contract E. From the
        // perspective
        // of the internal transaction, however, it looks like D calls D.
        long nrg = 1_000_000;
        long nrgPrice = 1;
        BigInteger nonce = BigInteger.TWO;
        byte[] input =
                ByteUtil.merge(Hex.decode("5cce9fc2"), E.toByteArray()); // use CALLCODE on E.
        input = ByteUtil.merge(input, new DataWord(0).getData()); // pass in 'n' also.

        AionTransaction tx =
                AionTransaction.create(
                        deployerKey,
                        nonce.toByteArray(),
                        D,
                        BigInteger.ZERO.toByteArray(),
                        input,
                        nrg,
                        nrgPrice,
                        TransactionTypes.DEFAULT);
        assertEquals(deployer, tx.getSenderAddress());
        assertEquals(D, tx.getDestinationAddress());

        BlockContext context =
                blockchain.createNewBlockContext(
                        blockchain.getBestBlock(), Collections.singletonList(tx), false);
        AionTxExecSummary summary = executeTransaction(tx, context.block, repo);
        assertEquals("", summary.getReceipt().getError());
        assertEquals(summary.getNrgUsed().longValue(), summary.getNrgUsed().longValue());

        // We expect that the internal transaction is sent from D to D.
        List<InternalTransaction> internalTxs = summary.getInternalTransactions();
        assertEquals(1, internalTxs.size());
        assertEquals(D, internalTxs.get(0).sender);
        assertEquals(D, internalTxs.get(0).destination);
    }

    @Test
    public void testCallcodeValueTransfer() throws IOException, VMException {
        RepositoryCache repo = blockchain.getRepository().startTracking();
        AionAddress D = deployContract(repo, "D", "D.sol", BigInteger.ZERO);
        AionAddress E = deployContract(repo, "E", "D.sol", BigInteger.ZERO);

        BigInteger balanceDeployer = repo.getBalance(deployer);
        BigInteger balanceD = repo.getBalance(D);
        BigInteger balanceE = repo.getBalance(E);

        // Deployer calls contract D which performs CALLCODE to call contract E.
        long nrg = 1_000_000;
        long nrgPrice = 1;
        BigInteger value = new BigInteger("2387653");
        BigInteger nonce = BigInteger.TWO;
        byte[] input =
                ByteUtil.merge(Hex.decode("5cce9fc2"), E.toByteArray()); // use CALLCODE on E.
        input = ByteUtil.merge(input, new DataWord(0).getData()); // pass in 'n' also.

        AionTransaction tx =
                AionTransaction.create(
                        deployerKey,
                        nonce.toByteArray(),
                        D,
                        value.toByteArray(),
                        input,
                        nrg,
                        nrgPrice,
                        TransactionTypes.DEFAULT);
        assertEquals(deployer, tx.getSenderAddress());
        assertEquals(D, tx.getDestinationAddress());

        BlockContext context =
                blockchain.createNewBlockContext(
                        blockchain.getBestBlock(), Collections.singletonList(tx), false);
        AionTxExecSummary summary = executeTransaction(tx, context.block, repo);
        assertEquals("", summary.getReceipt().getError());
        assertEquals(summary.getNrgUsed().longValue(), summary.getNrgUsed().longValue());

        // We expect that deployer paid the txCost and sent value. We expect that D received value.
        // We expect E had no value change.
        BigInteger txCost = BigInteger.valueOf(summary.getNrgUsed().longValue() * nrgPrice);
        assertEquals(balanceDeployer.subtract(value).subtract(txCost), repo.getBalance(deployer));
        assertEquals(balanceD.add(value), repo.getBalance(D));
        assertEquals(balanceE, repo.getBalance(E));
    }

    // ===================================== test DELEGATECALL =====================================

    @Test
    public void testDelegateCallStorage() throws IOException, VMException {
        RepositoryCache repo = blockchain.getRepository().startTracking();
        AionAddress D = deployContract(repo, "D", "D.sol", BigInteger.ZERO);
        AionAddress E = deployContract(repo, "E", "D.sol", BigInteger.ZERO);
        BigInteger n = new BigInteger("23786523");

        // Deployer calls contract D which performs DELEGATECALL to call contract E.
        long nrg = 1_000_000;
        long nrgPrice = 1;
        BigInteger value = new BigInteger("4364463");
        BigInteger nonce = BigInteger.TWO;
        byte[] input =
                ByteUtil.merge(Hex.decode("32817e1d"), E.toByteArray()); // use DELEGATECALL on E.
        input = ByteUtil.merge(input, new DataWord(n).getData()); // pass in 'n' also.

        AionTransaction tx =
                AionTransaction.create(
                        deployerKey,
                        nonce.toByteArray(),
                        D,
                        value.toByteArray(),
                        input,
                        nrg,
                        nrgPrice,
                        TransactionTypes.DEFAULT);
        assertEquals(deployer, tx.getSenderAddress());
        assertEquals(D, tx.getDestinationAddress());

        BlockContext context =
                blockchain.createNewBlockContext(
                        blockchain.getBestBlock(), Collections.singletonList(tx), false);
        AionTxExecSummary summary = executeTransaction(tx, context.block, repo);
        assertEquals("", summary.getReceipt().getError());
        assertEquals(summary.getNrgUsed().longValue(), summary.getNrgUsed().longValue());
        nonce = nonce.add(BigInteger.ONE);

        // When we call into contract D we should find its storage is modified so that 'n' is set.
        input = Hex.decode("3e955225");
        tx =
                AionTransaction.create(
                        deployerKey,
                        nonce.toByteArray(),
                        D,
                        BigInteger.ZERO.toByteArray(),
                        input,
                        nrg,
                        nrgPrice,
                        TransactionTypes.DEFAULT);
        assertEquals(deployer, tx.getSenderAddress());
        assertEquals(D, tx.getDestinationAddress());

        context =
                blockchain.createNewBlockContext(
                        blockchain.getBestBlock(), Collections.singletonList(tx), false);
        BigInteger inStore =
                new BigInteger(executeTransaction(tx, context.block, repo).getResult());
        assertEquals(n, inStore);
        nonce = nonce.add(BigInteger.ONE);

        // When we call into contract E we should find its storage is unmodified.
        tx =
                AionTransaction.create(
                        deployerKey,
                        nonce.toByteArray(),
                        E,
                        BigInteger.ZERO.toByteArray(),
                        input,
                        nrg,
                        nrgPrice,
                        TransactionTypes.DEFAULT);
        assertEquals(deployer, tx.getSenderAddress());
        assertEquals(E, tx.getDestinationAddress());

        context =
                blockchain.createNewBlockContext(
                        blockchain.getBestBlock(), Collections.singletonList(tx), false);
        inStore = new BigInteger(executeTransaction(tx, context.block, repo).getResult());
        assertEquals(BigInteger.ZERO, inStore);
    }

    @Test
    public void testDelegateCallActors() throws IOException, VMException {
        RepositoryCache repo = blockchain.getRepository().startTracking();
        AionAddress D = deployContract(repo, "D", "D.sol", BigInteger.ZERO);
        AionAddress E = deployContract(repo, "E", "D.sol", BigInteger.ZERO);
        BigInteger n = new BigInteger("23786523");

        // Deployer calls contract D which performs DELEGATECALL to call contract E.
        long nrg = 1_000_000;
        long nrgPrice = 1;
        BigInteger value = new BigInteger("4364463");
        BigInteger nonce = BigInteger.TWO;
        byte[] input =
                ByteUtil.merge(Hex.decode("32817e1d"), E.toByteArray()); // use DELEGATECALL on E.
        input = ByteUtil.merge(input, new DataWord(n).getData()); // pass in 'n' also.

        AionTransaction tx =
                AionTransaction.create(
                        deployerKey,
                        nonce.toByteArray(),
                        D,
                        value.toByteArray(),
                        input,
                        nrg,
                        nrgPrice,
                        TransactionTypes.DEFAULT);
        assertEquals(deployer, tx.getSenderAddress());
        assertEquals(D, tx.getDestinationAddress());

        BlockContext context =
                blockchain.createNewBlockContext(
                        blockchain.getBestBlock(), Collections.singletonList(tx), false);
        AionTxExecSummary summary = executeTransaction(tx, context.block, repo);
        assertEquals("", summary.getReceipt().getError());
        assertEquals(summary.getNrgUsed().longValue(), summary.getNrgUsed().longValue());

        // We expect there to be one internal transaction and it should look like deployer sent to
        // D.
        List<InternalTransaction> internalTxs = summary.getInternalTransactions();
        assertEquals(1, internalTxs.size());
        assertEquals(deployer, internalTxs.get(0).sender);
        assertEquals(D, internalTxs.get(0).destination);
    }

    @Test
    public void testDelegateCallValueTransfer() throws IOException, VMException {
        RepositoryCache repo = blockchain.getRepository().startTracking();
        AionAddress D = deployContract(repo, "D", "D.sol", BigInteger.ZERO);
        AionAddress E = deployContract(repo, "E", "D.sol", BigInteger.ZERO);
        BigInteger n = new BigInteger("23786523");

        BigInteger balanceDeployer = repo.getBalance(deployer);
        BigInteger balanceD = repo.getBalance(D);
        BigInteger balanceE = repo.getBalance(E);

        // Deployer calls contract D which performs DELEGATECALL to call contract E.
        long nrg = 1_000_000;
        long nrgPrice = 1;
        BigInteger value = new BigInteger("4364463");
        BigInteger nonce = BigInteger.TWO;
        byte[] input =
                ByteUtil.merge(Hex.decode("32817e1d"), E.toByteArray()); // use DELEGATECALL on E.
        input = ByteUtil.merge(input, new DataWord(n).getData()); // pass in 'n' also.

        AionTransaction tx =
                AionTransaction.create(
                        deployerKey,
                        nonce.toByteArray(),
                        D,
                        value.toByteArray(),
                        input,
                        nrg,
                        nrgPrice,
                        TransactionTypes.DEFAULT);
        assertEquals(deployer, tx.getSenderAddress());
        assertEquals(D, tx.getDestinationAddress());

        BlockContext context =
                blockchain.createNewBlockContext(
                        blockchain.getBestBlock(), Collections.singletonList(tx), false);
        AionTxExecSummary summary = executeTransaction(tx, context.block, repo);
        assertEquals("", summary.getReceipt().getError());
        assertEquals(summary.getNrgUsed().longValue(), summary.getNrgUsed().longValue());

        // We expect that deployer paid the tx cost and sent value. We expect that D received value.
        // We expect that E received nothing.
        BigInteger txCost = BigInteger.valueOf(summary.getNrgUsed().longValue() * nrgPrice);
        assertEquals(balanceDeployer.subtract(value).subtract(txCost), repo.getBalance(deployer));
        assertEquals(balanceD.add(value), repo.getBalance(D));
        assertEquals(balanceE, repo.getBalance(E));
    }

    // ============================= test CALL, CALLCODE, DELEGATECALL =============================

    @Test
    public void testOpcodesActors() throws IOException, VMException {
        RepositoryCache repo = blockchain.getRepository().startTracking();
        AionAddress callerContract = deployContract(repo, "Caller", "Opcodes.sol", BigInteger.ZERO);
        AionAddress calleeContract = deployContract(repo, "Callee", "Opcodes.sol", BigInteger.ZERO);

        System.err.println("Deployer: " + deployer);
        System.err.println("Caller: " + callerContract);
        System.err.println("Callee: " + calleeContract);

        long nrg = 1_000_000;
        long nrgPrice = 1;
        BigInteger nonce = BigInteger.TWO;
        byte[] input = ByteUtil.merge(Hex.decode("fc68521a"), calleeContract.toByteArray());
        AionTransaction tx =
                AionTransaction.create(
                        deployerKey,
                        nonce.toByteArray(),
                        callerContract,
                        BigInteger.ZERO.toByteArray(),
                        input,
                        nrg,
                        nrgPrice,
                        TransactionTypes.DEFAULT);

        BlockContext context =
                blockchain.createNewBlockContext(
                        blockchain.getBestBlock(), Collections.singletonList(tx), false);
        AionTxExecSummary summary = executeTransaction(tx, context.block, repo);
        assertEquals("", summary.getReceipt().getError());
        assertEquals(summary.getNrgUsed().longValue(), summary.getNrgUsed().longValue());

        // We examine the logs to determine the expected state. We expect to see
        // owner-caller-origin-data as follows for each opcode:
        //
        // CALL             -->     CALLEE-CALLER-DEPLOYER-ZERO
        // CALLCODE         -->     CALLER-CALLER-DEPLOYER-ZERO
        // DELEGATECALL     -->     CALLER-DEPLOYER-DEPLOYER-ZERO
        List<Log> logs = summary.getReceipt().getLogInfoList();
        assertEquals(3, logs.size());
        verifyLogData(logs.get(0).copyOfData(), calleeContract, callerContract, deployer);
        verifyLogData(logs.get(1).copyOfData(), callerContract, callerContract, deployer);
        verifyLogData(logs.get(2).copyOfData(), callerContract, deployer, deployer);
    }

    // ======================================= test SUICIDE ========================================

    @Test
    public void testSuicideRecipientExists() throws IOException, VMException {
        RepositoryCache repo = blockchain.getRepository().startTracking();
        BigInteger balance = new BigInteger("32522224");
        AionAddress recipient = new AionAddress(RandomUtils.nextBytes(AionAddress.LENGTH));
        repo.createAccount(recipient);

        AionAddress contract = deployContract(repo, "Suicide", "Suicide.sol", BigInteger.ZERO);
        repo.addBalance(contract, balance);

        BigInteger balanceDeployer = repo.getBalance(deployer);
        BigInteger balanceRecipient = repo.getBalance(recipient);
        assertEquals(balance, repo.getBalance(contract));

        long nrg = 1_000_000;
        long nrgPrice = 1;
        BigInteger nonce = BigInteger.ONE;
        byte[] input = ByteUtil.merge(Hex.decode("fc68521a"), recipient.toByteArray());
        AionTransaction tx =
                AionTransaction.create(
                        deployerKey,
                        nonce.toByteArray(),
                        contract,
                        BigInteger.ZERO.toByteArray(),
                        input,
                        nrg,
                        nrgPrice,
                        TransactionTypes.DEFAULT);

        BlockContext context =
                blockchain.createNewBlockContext(
                        blockchain.getBestBlock(), Collections.singletonList(tx), false);
        AionTxExecSummary summary = executeTransaction(tx, context.block, repo);
        assertEquals("", summary.getReceipt().getError());
        assertEquals(summary.getNrgUsed().longValue(), summary.getNrgUsed().longValue());

        // We expect that deployer paid the tx cost. We expect that all of the balance in the
        // contract has been transferred to recipient. We expect that the contract has been deleted.
        BigInteger txCost = BigInteger.valueOf(summary.getNrgUsed().longValue() * nrgPrice);
        assertEquals(balanceDeployer.subtract(txCost), repo.getBalance(deployer));
        assertEquals(balanceRecipient.add(balance), repo.getBalance(recipient));
        assertEquals(BigInteger.ZERO, repo.getBalance(contract));
        assertFalse(repo.hasAccountState(contract));
        assertEquals(1, summary.getDeletedAccounts().size());
        assertEquals(contract, summary.getDeletedAccounts().get(0));
    }

    @Test
    public void testSuicideRecipientNewlyCreated() throws IOException, VMException {
        RepositoryCache repo = blockchain.getRepository().startTracking();
        BigInteger balance = new BigInteger("32522224");
        AionAddress recipient = new AionAddress(RandomUtils.nextBytes(AionAddress.LENGTH));

        AionAddress contract = deployContract(repo, "Suicide", "Suicide.sol", BigInteger.ZERO);
        repo.addBalance(contract, balance);

        BigInteger balanceDeployer = repo.getBalance(deployer);
        BigInteger balanceRecipient = BigInteger.ZERO;
        assertEquals(balance, repo.getBalance(contract));

        long nrg = 1_000_000;
        long nrgPrice = 1;
        BigInteger nonce = BigInteger.ONE;
        byte[] input = ByteUtil.merge(Hex.decode("fc68521a"), recipient.toByteArray());
        AionTransaction tx =
                AionTransaction.create(
                        deployerKey,
                        nonce.toByteArray(),
                        contract,
                        BigInteger.ZERO.toByteArray(),
                        input,
                        nrg,
                        nrgPrice,
                        TransactionTypes.DEFAULT);

        BlockContext context =
                blockchain.createNewBlockContext(
                        blockchain.getBestBlock(), Collections.singletonList(tx), false);
        AionTxExecSummary summary = executeTransaction(tx, context.block, repo);
        assertEquals("", summary.getReceipt().getError());
        assertEquals(summary.getNrgUsed().longValue(), summary.getNrgUsed().longValue());

        // We expect that deployer paid the tx cost. We expect that a new account was created and
        // all of the balance in the contract has been transferred to it. We expect that the
        // contract has been deleted.
        BigInteger txCost = BigInteger.valueOf(summary.getNrgUsed().longValue() * nrgPrice);
        assertEquals(balanceDeployer.subtract(txCost), repo.getBalance(deployer));
        assertTrue(repo.hasAccountState(recipient));
        assertEquals(balanceRecipient.add(balance), repo.getBalance(recipient));
        assertEquals(BigInteger.ZERO, repo.getBalance(contract));
        assertFalse(repo.hasAccountState(contract));
        assertEquals(1, summary.getDeletedAccounts().size());
        assertEquals(contract, summary.getDeletedAccounts().get(0));
    }

    // <-------------------------------------------------------------------------------------------->

    /**
     * Deploys the contract named contractName in the file named contractFilename with value value.
     */
    private AionAddress deployContract(
            RepositoryCache repo, String contractName, String contractFilename, BigInteger value)
            throws IOException, VMException {

        byte[] deployCode = ContractUtils.getContractDeployer(contractFilename, contractName);
        long nrg = 1_000_000;
        long nrgPrice = 1;
        BigInteger nonce = repo.getNonce(deployer);
        AionTransaction tx =
                AionTransaction.create(
                        deployerKey,
                        nonce.toByteArray(),
                        null,
                        value.toByteArray(),
                        deployCode,
                        nrg,
                        nrgPrice,
                        TransactionTypes.DEFAULT);
        AionAddress contract =
                deployContract(
                        repo, tx, contractName, contractFilename, value, nrg, nrgPrice, nonce);
        deployerBalance = repo.getBalance(deployer);
        return contract;
    }

    /**
     * Deploys a contract named contractName in a file named contractFilename and checks the state
     * of the deployed contract and the contract deployer.
     *
     * <p>Returns the address of the newly deployed contract.
     */
    private AionAddress deployContract(
            RepositoryCache repo,
            AionTransaction tx,
            String contractName,
            String contractFilename,
            BigInteger value,
            long nrg,
            long nrgPrice,
            BigInteger expectedNonce)
            throws IOException, VMException {

        assertTrue(tx.isContractCreationTransaction());
        assertEquals(deployerBalance, repo.getBalance(deployer));
        assertEquals(expectedNonce, repo.getNonce(deployer));

        BlockContext context =
                blockchain.createNewBlockContext(
                        blockchain.getBestBlock(), Collections.singletonList(tx), false);

        AionTxExecSummary summary = executeTransaction(tx, context.block, repo);
        assertEquals("", summary.getReceipt().getError());
        assertNotEquals(nrg, summary.getNrgUsed().longValue());

        AionAddress contract = TxUtil.calculateContractAddress(tx);
        checkStateOfNewContract(repo, contractName, contractFilename, contract, value);
        checkStateOfDeployer(
                repo,
                deployerBalance,
                summary.getNrgUsed().longValue(),
                nrgPrice,
                value,
                expectedNonce);
        return contract;
    }

    /**
     * Checks that the newly deployed contract in file contractFilename and named contractName is
     * deployed at address contractAddr with the expected body code and a zero nonce and balance
     * equal to whatever value was transferred to it when deployed.
     */
    private void checkStateOfNewContract(
            RepositoryCache repo,
            String contractName,
            String contractFilename,
            AionAddress contractAddr,
            BigInteger valueTransferred)
            throws IOException {

        byte[] expectedBodyCode = ContractUtils.getContractBody(contractFilename, contractName);
        assertArrayEquals(expectedBodyCode, repo.getCode(contractAddr));
        assertEquals(valueTransferred, repo.getBalance(contractAddr));
        assertEquals(BigInteger.ZERO, repo.getNonce(contractAddr));
    }

    /**
     * Checks the state of the deployer after a successful contract deployment. In this case we
     * expect the deployer's nonce to have incremented by one and their new balance to be equal to
     * the prior balance minus the tx cost and the value transferred.
     */
    private void checkStateOfDeployer(
            RepositoryCache repo,
            BigInteger priorBalance,
            long nrgUsed,
            long nrgPrice,
            BigInteger value,
            BigInteger priorNonce) {

        assertEquals(priorNonce.add(BigInteger.ONE), repo.getNonce(deployer));
        BigInteger txCost = BigInteger.valueOf(nrgUsed * nrgPrice);
        assertEquals(priorBalance.subtract(txCost).subtract(value), repo.getBalance(deployer));
    }

    /**
     * Verifies the log data for a call to f() in the Caller contract in the file Opcodes.sol is a
     * byte array consisting of the bytes of owner then caller then origin then finally 16 zero
     * bytes.
     */
    private void verifyLogData(
            byte[] data, AionAddress owner, AionAddress caller, AionAddress origin) {
        assertArrayEquals(Arrays.copyOfRange(data, 0, AionAddress.LENGTH), owner.toByteArray());
        assertArrayEquals(
                Arrays.copyOfRange(data, AionAddress.LENGTH, AionAddress.LENGTH * 2),
                caller.toByteArray());
        assertArrayEquals(
                Arrays.copyOfRange(data, AionAddress.LENGTH * 2, AionAddress.LENGTH * 3),
                origin.toByteArray());
        assertArrayEquals(
                Arrays.copyOfRange(data, data.length - DataWord.BYTES, data.length),
                DataWord.ZERO.getData());
    }

    private AionTxExecSummary executeTransaction(
            AionTransaction tx, Block block, RepositoryCache repo) throws VMException {
        return BulkExecutor.executeTransactionWithNoPostExecutionWork(
                block.getDifficulty(),
                block.getNumber(),
                block.getTimestamp(),
                block.getNrgLimit(),
                block.getCoinbase(),
                tx,
                repo,
                false,
                true,
                false,
                false,
                LOGGER_VM,
                BlockCachingContext.PENDING,
                block.getNumber() - 1);
    }
}
