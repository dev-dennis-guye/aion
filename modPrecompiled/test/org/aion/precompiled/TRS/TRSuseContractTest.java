package org.aion.precompiled.TRS;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.aion.base.type.Address;
import org.aion.base.util.ByteUtil;
import org.aion.mcf.vm.AbstractExecutionResult.ResultCode;
import org.aion.crypto.ECKeyFac;
import org.aion.precompiled.ContractExecutionResult;
import org.aion.precompiled.DummyRepo;
import org.aion.precompiled.contracts.TRS.AbstractTRS;
import org.aion.precompiled.contracts.TRS.TRSuseContract;
import org.aion.precompiled.type.StatefulPrecompiledContract;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the TRSuseContract API.
 */
public class TRSuseContractTest extends TRShelpers {
    private static final BigInteger DEFAULT_BALANCE = BigInteger.TEN;

    @Before
    public void setup() {
        repo = new DummyRepo();
        ((DummyRepo) repo).storageErrorReturn = null;
        tempAddrs = new ArrayList<>();
        repo.addBalance(AION, BigInteger.ONE);
    }

    @After
    public void tearDown() {
        for (Address acct : tempAddrs) {
            repo.deleteAccount(acct);
        }
        tempAddrs = null;
        repo = null;
    }

    // <-----------------------------------HELPER METHODS BELOW------------------------------------>

    // Returns a properly formatted byte array to be used as input for the refund operation, to
    // refund the maximum allowable amount.
    private byte[] getMaxRefundInput(Address contract, Address account) {
        byte[] input = new byte[193];
        input[0] = 0x4;
        System.arraycopy(contract.toBytes(), 0, input, 1, Address.ADDRESS_LEN);
        System.arraycopy(account.toBytes(), 0, input, 33, Address.ADDRESS_LEN);
        for (int i = 65; i < 193; i++) {
            input[i] = (byte) 0xFF;
        }
        return input;
    }

    // Returns a properly formatted byte array to be used as input for the deposit operation, to
    // deposit the maximum allowable amount.
    private byte[] getMaxDepositInput(Address contract) {
        byte[] input = new byte[161];
        input[0] = 0x0;
        System.arraycopy(contract.toBytes(), 0, input, 1, Address.ADDRESS_LEN);
        for (int i = 33; i < 161; i++) {
            input[i] = (byte) 0xFF;
        }
        return input;
    }

    // Returns a TRS contract address that has numDepositors depositors in it (owner does not deposit)
    // each with DEFAULT_BALANCE deposit balance.
    private Address getContractMultipleDepositors(int numDepositors, Address owner, boolean isTest,
        boolean isDirectDeposit, int periods, BigInteger percent, int precision) {

        Address contract = createTRScontract(owner, isTest, isDirectDeposit, periods, percent, precision);
        byte[] input = getDepositInput(contract, DEFAULT_BALANCE);
        for (int i = 0; i < numDepositors; i++) {
            Address acct = getNewExistentAccount(DEFAULT_BALANCE);
            if (!newTRSuseContract(acct).execute(input, COST).getResultCode().equals(ResultCode.SUCCESS)) {
                fail("Depositor #" + i + " failed to deposit!");
            }
        }
        return contract;
    }

    // Returns a TRS contract address that has numDepositors depositors in it (owner does not deposit)
    // each with DEFAULT_BALANCE deposit balance. Owner uses depositFor to deposit for depositors.
    private Address getContractMultipleDepositorsUsingDepositFor(int numDepositors, Address owner,
        boolean isTest, int periods, BigInteger percent, int precision) {

        Address contract = createTRScontract(owner, isTest, false, periods, percent, precision);
        AbstractTRS trs = newTRSuseContract(owner);
        for (int i = 0; i < numDepositors; i++) {
            Address acct = getNewExistentAccount(BigInteger.ZERO);
            byte[] input = getDepositForInput(contract, acct, DEFAULT_BALANCE);
            if (!trs.execute(input, COST).getResultCode().equals(ResultCode.SUCCESS)) {
                fail("Depositor #" + i + " failed to deposit!");
            }
        }
        return contract;
    }

    // Returns the maximum amount that can be deposited in a single deposit call.
    private BigInteger getMaxOneTimeDeposit() {
        return BigInteger.TWO.pow(1024).subtract(BigInteger.ONE);
    }

    // Returns the maximum amount that a single account can deposit into a TRS contract.
    private BigInteger getMaxTotalDeposit() {
        return BigInteger.TWO.pow(4096).subtract(BigInteger.ONE);
    }

    // Returns true only if account is a valid account in contract.
    private boolean accountIsValid(AbstractTRS trs, Address contract, Address account) {
        try {
            return AbstractTRS.accountIsValid(trs.getListNextBytes(contract, account));
        } catch (Exception e) {
            // Since we possibly call on a non-existent account.
            return false;
        }
    }

    // Returns true only if account is eligible to use the special one-off withdrawal event.
    private boolean accountIsEligibleForSpecial(TRSuseContract trs, Address contract, Address account) {
        return trs.accountIsEligibleForSpecial(contract, account);
    }

    // Returns the last period in which account made a withdrawal or -1 if bad contract or account.
    private int getAccountLastWithdrawalPeriod(AbstractTRS trs, Address contract, Address account) {
        return trs.getAccountLastWithdrawalPeriod(contract, account);
    }

    // Checks that each account is paid out correctly when they withdraw in a non-final period.
    private void checkPayoutsNonFinal(AbstractTRS trs, Address contract, int numDepositors,
        BigInteger deposits, BigInteger bonus, BigDecimal percent, int periods, int currPeriod) {

        Set<Address> contributors = getAllDepositors(trs, contract);
        BigInteger total = deposits.multiply(BigInteger.valueOf(numDepositors));
        BigDecimal fraction = BigDecimal.ONE.divide(new BigDecimal(numDepositors), 18, RoundingMode.HALF_DOWN);

        for (Address acc : contributors) {
            BigInteger extraShare = getExtraShare(trs, contract, acc, fraction, currPeriod);
            BigInteger amt = expectedAmtFirstWithdraw(trs, contract, deposits, total, bonus, percent, periods);

            byte[] input = getWithdrawInput(contract);
            assertEquals(ResultCode.SUCCESS, newTRSuseContract(acc).execute(input, COST).getResultCode());
            assertEquals(amt.add(extraShare), repo.getBalance(acc));
        }
    }

    // Checks that each account is paid out correctly when they withdraw in a final period.
    private void checkPayoutsFinal(AbstractTRS trs, Address contract, int numDepositors, BigInteger deposits,
        BigInteger bonus, BigInteger extra) {

        Set<Address> contributors = getAllDepositors(trs, contract);
        BigDecimal fraction = BigDecimal.ONE.divide(new BigDecimal(numDepositors), 18, RoundingMode.HALF_DOWN);
        BigInteger extraShare = fraction.multiply(new BigDecimal(extra)).toBigInteger();
        BigInteger amt = (fraction.multiply(new BigDecimal(bonus))).add(new BigDecimal(deposits)).toBigInteger();
        BigInteger collected = amt.add(extraShare);

        for (Address acc : contributors) {
            byte[] input = getWithdrawInput(contract);
            assertEquals(ResultCode.SUCCESS, newTRSuseContract(acc).execute(input, COST).getResultCode());
            assertEquals(collected, repo.getBalance(acc));
        }

        // Verify that the contract has enough funds to pay out.
        BigInteger contractTotal = deposits.multiply(BigInteger.valueOf(numDepositors)).add(bonus).add(extra);
        assertTrue(collected.multiply(BigInteger.valueOf(numDepositors)).compareTo(contractTotal) <= 0);
    }

    // <----------------------------------MISCELLANEOUS TESTS-------------------------------------->

    @Test(expected=NullPointerException.class)
    public void testCreateNullCaller() {
        newTRSuseContract(null);
    }

    @Test
    public void testCreateNullInput() {
        TRSuseContract trs = newTRSuseContract(getNewExistentAccount(BigInteger.ZERO));
        ContractExecutionResult res = trs.execute(null, COST);
        assertEquals(ResultCode.INTERNAL_ERROR, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
    }

    @Test
    public void testCreateEmptyInput() {
        TRSuseContract trs = newTRSuseContract(getNewExistentAccount(BigInteger.ZERO));
        ContractExecutionResult res = trs.execute(ByteUtil.EMPTY_BYTE_ARRAY, COST);
        assertEquals(ResultCode.INTERNAL_ERROR, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
    }

    @Test
    public void testInsufficientNrg() {
        Address addr = getNewExistentAccount(BigInteger.ZERO);
        TRSuseContract trs = newTRSuseContract(addr);
        byte[] input = getDepositInput(addr, BigInteger.ZERO);
        ContractExecutionResult res;
        for (int i = 0; i <= useCurrMaxOp; i++) {
            res = trs.execute(input, COST - 1);
            assertEquals(ResultCode.OUT_OF_NRG, res.getResultCode());
            assertEquals(0, res.getNrgLeft());
        }
    }

    @Test
    public void testTooMuchNrg() {
        Address addr = getNewExistentAccount(BigInteger.ZERO);
        TRSuseContract trs = newTRSuseContract(addr);
        byte[] input = getDepositInput(addr, BigInteger.ZERO);
        ContractExecutionResult res;
        for (int i = 0; i <= useCurrMaxOp; i++) {
            res = trs.execute(input, StatefulPrecompiledContract.TX_NRG_MAX + 1);
            assertEquals(ResultCode.INVALID_NRG_LIMIT, res.getResultCode());
            assertEquals(0, res.getNrgLeft());
        }
    }

    @Test
    public void testInvalidOperation() {
        //TODO - need all ops implemented first
    }

    // <-------------------------------------DEPOSIT TRS TESTS------------------------------------->

    @Test
    public void testDepositInputTooShort() {
        // Test on minimum too-small amount.
        TRSuseContract trs = newTRSuseContract(getNewExistentAccount(BigInteger.ZERO));
        byte[] input = new byte[1];
        ContractExecutionResult res = trs.execute(input, COST);
        assertEquals(ResultCode.INTERNAL_ERROR, res.getResultCode());
        assertEquals(0, res.getNrgLeft());

        // Test on maximum too-small amount.
        input = new byte[160];
        res = trs.execute(input, COST);
        assertEquals(ResultCode.INTERNAL_ERROR, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
    }

    @Test
    public void testDepositInputTooLong() {
        TRSuseContract trs = newTRSuseContract(getNewExistentAccount(BigInteger.ZERO));
        byte[] input = new byte[162];
        ContractExecutionResult res = trs.execute(input, COST);
        assertEquals(ResultCode.INTERNAL_ERROR, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
    }

    @Test
    public void testDepositToNonExistentContract() {
        // Test on contract address actually an account address.
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        TRSuseContract trs = newTRSuseContract(acct);
        byte[] input = getDepositInput(acct, BigInteger.TWO);
        ContractExecutionResult res = trs.execute(input, COST);
        assertEquals(ResultCode.INTERNAL_ERROR, res.getResultCode());
        assertEquals(0, res.getNrgLeft());

        // Test on contract address looks like a legit TRS address (proper prefix).
        byte[] addr = new byte[Address.ADDRESS_LEN];
        System.arraycopy(acct.toBytes(), 0, addr, 0, Address.ADDRESS_LEN);
        addr[0] = (byte) 0xC0;

        input = getDepositInput(Address.wrap(addr), BigInteger.TWO);
        res = trs.execute(input, COST);
        assertEquals(ResultCode.INTERNAL_ERROR, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
    }

    @Test
    public void testDepositInsufficientBalance() {
        // Test not in test mode.
        // Test on minimum too-large amount.
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        TRSuseContract trs = newTRSuseContract(acct);
        Address contract = createTRScontract(acct, false, true, 1, BigInteger.ZERO, 0);

        byte[] input = getDepositInput(contract, DEFAULT_BALANCE.add(BigInteger.ONE));
        ContractExecutionResult res = trs.execute(input, COST);
        assertEquals(ResultCode.INSUFFICIENT_BALANCE, res.getResultCode());
        assertEquals(0, res.getNrgLeft());

        // Test on maximum too-large amount.
        input = getMaxDepositInput(contract);
        res = trs.execute(input, COST);
        assertEquals(ResultCode.INSUFFICIENT_BALANCE, res.getResultCode());
        assertEquals(0, res.getNrgLeft());

        // Test in test mode.
        // Test on minimum too-large amount.
        contract = createTRScontract(AION, true, true, 1, BigInteger.ZERO, 0);
        input = getDepositInput(contract, DEFAULT_BALANCE.add(BigInteger.ONE));
        res = trs.execute(input, COST);
        assertEquals(ResultCode.INSUFFICIENT_BALANCE, res.getResultCode());
        assertEquals(0, res.getNrgLeft());

        // Test on maximum too-large amount.
        input = getMaxDepositInput(contract);
        res = trs.execute(input, COST);
        assertEquals(ResultCode.INSUFFICIENT_BALANCE, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
    }

    @Test
    public void testDepositDirectDepositsDisabledCallerIsOwner() {
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        TRSuseContract trs = newTRSuseContract(acct);
        Address contract = createTRScontract(acct, false, false, 1, BigInteger.ZERO, 0);

        byte[] input = getDepositInput(contract, BigInteger.TWO);
        ContractExecutionResult res = trs.execute(input, COST);
        assertEquals(ResultCode.SUCCESS, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
        assertEquals(BigInteger.TWO, getTotalBalance(trs, contract));
    }

    @Test
    public void testDepositDirectDepositsDisabledCallerNotOwner() {
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address owner = getNewExistentAccount(BigInteger.ZERO);
        Address contract = createTRScontract(owner, false, false, 1, BigInteger.ZERO, 0);

        TRSuseContract trs = newTRSuseContract(acct);
        byte[] input = getDepositInput(contract, BigInteger.TWO);
        ContractExecutionResult res = trs.execute(input, COST);
        assertEquals(ResultCode.INTERNAL_ERROR, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
    }

    @Test
    public void testDepositCallerIsContract() {
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, false, 1, BigInteger.ZERO, 0);

        TRSuseContract trs = newTRSuseContract(contract);
        byte[] input = getDepositInput(contract, BigInteger.TWO);
        ContractExecutionResult res = trs.execute(input, COST);
        assertEquals(ResultCode.INTERNAL_ERROR, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
    }

    @Test
    public void testDepositZero() {
        // Test zero deposit with zero balance.
        Address acct = getNewExistentAccount(BigInteger.ZERO);
        TRSuseContract trs = newTRSuseContract(acct);
        Address contract = createTRScontract(acct, false, true, 1, BigInteger.ZERO, 0);

        byte[] input = getDepositInput(contract, BigInteger.ZERO);
        ContractExecutionResult res = trs.execute(input, COST);
        assertEquals(ResultCode.SUCCESS, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
        assertFalse(getDepositBalance(trs, contract, acct).compareTo(BigInteger.ZERO) > 0);
        assertEquals(BigInteger.ZERO, getTotalBalance(trs, contract));

        // Test zero deposit with non-zero balance.
        acct = getNewExistentAccount(DEFAULT_BALANCE);
        trs = newTRSuseContract(acct);
        contract = createTRScontract(acct, false, true, 1, BigInteger.ZERO, 0);

        input = getDepositInput(contract, BigInteger.ZERO);
       res = trs.execute(input, COST);
        assertEquals(ResultCode.SUCCESS, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
        assertFalse(getDepositBalance(trs, contract, acct).compareTo(BigInteger.ZERO) > 0);
        assertEquals(BigInteger.ZERO, getTotalBalance(trs, contract));
    }

    @Test
    public void testDepositOne() {
        // Test deposit with one balance.
        Address acct = getNewExistentAccount(BigInteger.ONE);
        TRSuseContract trs = newTRSuseContract(acct);
        Address contract = createTRScontract(acct, false, true, 1, BigInteger.ZERO, 0);

        byte[] input = getDepositInput(contract, BigInteger.ONE);
        ContractExecutionResult res = trs.execute(input, COST);
        assertEquals(ResultCode.SUCCESS, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
        assertTrue(getDepositBalance(trs, contract, acct).compareTo(BigInteger.ZERO) > 0);
        assertEquals(BigInteger.ZERO, repo.getBalance(acct));
        assertEquals(BigInteger.ONE, getDepositBalance(trs, contract, acct));
        assertEquals(BigInteger.ONE, getTotalBalance(trs, contract));

        // Test deposit with balance larger than one.
        acct = getNewExistentAccount(DEFAULT_BALANCE);
        trs = newTRSuseContract(acct);
        contract = createTRScontract(acct, false, true, 1, BigInteger.ZERO, 0);

        input = getDepositInput(contract, BigInteger.ONE);
        res = trs.execute(input, COST);
        assertEquals(ResultCode.SUCCESS, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
        assertTrue(getDepositBalance(trs, contract, acct).compareTo(BigInteger.ZERO) > 0);
        assertEquals(DEFAULT_BALANCE.subtract(BigInteger.ONE), repo.getBalance(acct));
        assertEquals(BigInteger.ONE, getDepositBalance(trs, contract, acct));
        assertEquals(BigInteger.ONE, getTotalBalance(trs, contract));
    }

    @Test
    public void testDepositFullBalance() {
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        TRSuseContract trs = newTRSuseContract(acct);
        Address contract = createTRScontract(acct, false, true, 1, BigInteger.ZERO, 0);

        byte[] input = getDepositInput(contract, DEFAULT_BALANCE);
        ContractExecutionResult res = trs.execute(input, COST);
        assertEquals(ResultCode.SUCCESS, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
        assertTrue(getDepositBalance(trs, contract, acct).compareTo(BigInteger.ZERO) > 0);
        assertEquals(BigInteger.ZERO, repo.getBalance(acct));
        assertEquals(DEFAULT_BALANCE, getDepositBalance(trs, contract, acct));
        assertEquals(DEFAULT_BALANCE, getTotalBalance(trs, contract));

        // Test on max deposit amount.
        BigInteger max = getMaxOneTimeDeposit();
        acct = getNewExistentAccount(max);
        trs = newTRSuseContract(acct);
        contract = createTRScontract(acct, false, true, 1, BigInteger.ZERO, 0);

        input = getMaxDepositInput(contract);
        res = trs.execute(input, COST);
        assertEquals(ResultCode.SUCCESS, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
        assertTrue(getDepositBalance(trs, contract, acct).compareTo(BigInteger.ZERO) > 0);
        assertEquals(BigInteger.ZERO, repo.getBalance(acct));
        assertEquals(max, getDepositBalance(trs, contract, acct));
        assertEquals(max, getTotalBalance(trs, contract));
    }

    @Test
    public void testDepositMaxOneTimeAmount() {
        BigInteger max = getMaxOneTimeDeposit();
        Address acct = getNewExistentAccount(max.add(DEFAULT_BALANCE));
        TRSuseContract trs = newTRSuseContract(acct);
        Address contract = createTRScontract(acct, false, true, 1, BigInteger.ZERO, 0);

        byte[] input = getMaxDepositInput(contract);
        ContractExecutionResult res = trs.execute(input, COST);
        assertEquals(ResultCode.SUCCESS, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
        assertTrue(getDepositBalance(trs, contract, acct).compareTo(BigInteger.ZERO) > 0);
        assertEquals(DEFAULT_BALANCE, repo.getBalance(acct));
        assertEquals(max, getDepositBalance(trs, contract, acct));
        assertEquals(max, getTotalBalance(trs, contract));
    }

    @Test
    public void testDepositMultipleTimes() {
        BigInteger max = getMaxOneTimeDeposit();
        Address acct = getNewExistentAccount(max);
        TRSuseContract trs = newTRSuseContract(acct);
        Address contract = createTRScontract(acct, false, true, 1, BigInteger.ZERO, 0);

        BigInteger amt = new BigInteger("123456");
        BigInteger left = max;
        BigInteger depo = BigInteger.ZERO;
        byte[] input = getDepositInput(contract, amt);
        for (int i = 0; i < 7; i++) {
            ContractExecutionResult res = trs.execute(input, COST);
            assertEquals(ResultCode.SUCCESS, res.getResultCode());
            assertEquals(0, res.getNrgLeft());
            left = left.subtract(amt);
            depo = depo.add(amt);
        }

        assertEquals(left, repo.getBalance(acct));
        assertEquals(depo, getDepositBalance(trs, contract, acct));
        assertEquals(depo, getTotalBalance(trs, contract));
    }

    @Test
    public void testDepositMaxMultipleTimes() {
        // We do a large number of max deposits. There is no way we can test hitting the absolute
        // maximum but we can still hit it a good number of times.
        BigInteger maxTotal = getMaxTotalDeposit();
        BigInteger max = getMaxOneTimeDeposit();
        Address acct = getNewExistentAccount(maxTotal);
        TRSuseContract trs = newTRSuseContract(acct);
        Address contract = createTRScontract(acct, false, true, 1, BigInteger.ZERO, 0);

        BigInteger left = maxTotal;
        BigInteger depo = BigInteger.ZERO;
        byte[] input = getMaxDepositInput(contract);
        for (int i = 0; i < 100; i++) {
            ContractExecutionResult res = trs.execute(input, COST);
            assertEquals(ResultCode.SUCCESS, res.getResultCode());
            assertEquals(0, res.getNrgLeft());
            left = left.subtract(max);
            depo = depo.add(max);
        }

        assertEquals(left, repo.getBalance(acct));
        assertEquals(depo, getDepositBalance(trs, contract, acct));
        assertEquals(depo, getTotalBalance(trs, contract));
    }

    @Test
    public void testDepositMultipleDepositors() {
        BigInteger max = getMaxOneTimeDeposit();
        Address acct1 = getNewExistentAccount(max);
        Address acct2 = getNewExistentAccount(max);
        Address acct3 = getNewExistentAccount(max);
        BigInteger acct1Bal = max;
        BigInteger acct2Bal = max;
        BigInteger acct3Bal = max;
        BigInteger acct1Depo = BigInteger.ZERO;
        BigInteger acct2Depo = BigInteger.ZERO;
        BigInteger acct3Depo = BigInteger.ZERO;

        Address contract = createTRScontract(acct1, false, true, 1, BigInteger.ZERO, 0);
        BigInteger amt1 = new BigInteger("123456");
        BigInteger amt2 = new BigInteger("4363123");
        BigInteger amt3 = new BigInteger("8597455434");

        byte[] input1 = getDepositInput(contract, amt1);
        byte[] input2 = getDepositInput(contract, amt2);
        byte[] input3 = getDepositInput(contract, amt3);

        for (int i = 0; i < 10; i++) {
            newTRSuseContract(acct1).execute(input1, COST);
            newTRSuseContract(acct2).execute(input2, COST);
            newTRSuseContract(acct3).execute(input3, COST);
            newTRSuseContract(acct1).execute(input3, COST);

            acct1Bal = acct1Bal.subtract(amt1).subtract(amt3);
            acct1Depo = acct1Depo.add(amt1).add(amt3);
            acct2Bal = acct2Bal.subtract(amt2);
            acct2Depo = acct2Depo.add(amt2);
            acct3Bal = acct3Bal.subtract(amt3);
            acct3Depo = acct3Depo.add(amt3);
        }

        assertEquals(acct1Bal, repo.getBalance(acct1));
        assertEquals(acct2Bal, repo.getBalance(acct2));
        assertEquals(acct3Bal, repo.getBalance(acct3));

        TRSuseContract trs = newTRSuseContract(acct1);
        assertEquals(acct1Depo, getDepositBalance(trs, contract, acct1));
        assertEquals(acct2Depo, getDepositBalance(trs, contract, acct2));
        assertEquals(acct3Depo, getDepositBalance(trs, contract, acct3));
        assertEquals(acct1Depo.add(acct2Depo).add(acct3Depo), getTotalBalance(trs, contract));
    }

    @Test
    public void testDepositThatCausesOverflow() {
        // First we deposit 2**31 - 1 and then deposit 1 to overflow into next row.
        BigInteger total = BigInteger.TWO.pow(255);
        Address acct = getNewExistentAccount(total);
        BigInteger amount = total.subtract(BigInteger.ONE);
        Address contract = createTRScontract(acct, false, true, 1, BigInteger.ZERO, 0);
        TRSuseContract trs = newTRSuseContract(acct);
        byte[] input = getDepositInput(contract, amount);
        trs.execute(input, COST);

        input = getDepositInput(contract, BigInteger.ONE);
        ContractExecutionResult res = trs.execute(input, COST);
        assertEquals(ResultCode.SUCCESS, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
        assertEquals(BigInteger.ZERO, repo.getBalance(acct));
        assertEquals(total, getDepositBalance(trs, contract, acct));
        assertEquals(total, getTotalBalance(trs, contract));

        // Second we deposit 1 and then deposit 2**31 - 1 to overflow into next row.
        acct = getNewExistentAccount(total);
        amount = total.subtract(BigInteger.ONE);
        contract = createTRScontract(acct, false, true, 1, BigInteger.ZERO, 0);
        trs = newTRSuseContract(acct);
        input = getDepositInput(contract, BigInteger.ONE);
        trs.execute(input, COST);

        input = getDepositInput(contract, amount);
        res = trs.execute(input, COST);
        assertEquals(ResultCode.SUCCESS, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
        assertEquals(BigInteger.ZERO, repo.getBalance(acct));
        assertEquals(total, getDepositBalance(trs, contract, acct));
        assertEquals(total, getTotalBalance(trs, contract));
    }

    @Test
    public void testDepositNumRowsWhenAllRowsFull() {
        BigInteger total = BigInteger.TWO.pow(255);
        Address acct = getNewExistentAccount(total);
        Address contract = createTRScontract(acct, false, true, 1, BigInteger.ZERO, 0);
        TRSuseContract trs = newTRSuseContract(acct);
        byte[] input = getDepositInput(contract, total);
        trs.execute(input, COST);

        int rows = repo.getStorageValue(contract, newIDataWord(acct.toBytes())).getData()[0] & 0x0F;
        assertEquals(1, rows);
    }

    @Test
    public void testDepositNumRowsWhenOneRowHasOneNonZeroByte() {
        BigInteger total = BigInteger.TWO.pow(256);
        Address acct = getNewExistentAccount(total);
        Address contract = createTRScontract(acct, false, true, 1, BigInteger.ZERO, 0);
        TRSuseContract trs = newTRSuseContract(acct);
        byte[] input = getDepositInput(contract, total);
        trs.execute(input, COST);

        int rows = repo.getStorageValue(contract, newIDataWord(acct.toBytes())).getData()[0] & 0x0F;
        assertEquals(2, rows);
    }

    @Test
    public void testDepositWhileTRSisLocked() {
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createAndLockTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);
        TRSuseContract trs = newTRSuseContract(acct);
        byte[] input = getDepositInput(contract, BigInteger.ONE);
        ContractExecutionResult res = trs.execute(input, COST);
        assertEquals(ResultCode.INTERNAL_ERROR, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
    }

    @Test
    public void testDepositWhileTRSisLive() {
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createLockedAndLiveTRScontract(acct, false, true,
            1, BigInteger.ZERO, 0);
        TRSuseContract trs = newTRSuseContract(acct);
        byte[] input = getDepositInput(contract, BigInteger.ONE);
        ContractExecutionResult res = trs.execute(input, COST);
        assertEquals(ResultCode.INTERNAL_ERROR, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
    }

    @Test
    public void testAccountIsValidPriorToDeposit() {
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);
        TRSuseContract trs = newTRSuseContract(acct);
        assertFalse(accountIsValid(trs, contract, acct));
    }

    @Test
    public void testAccountIsValidAfterDeposit() {
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);
        TRSuseContract trs = newTRSuseContract(acct);
        byte[] input = getDepositInput(contract, BigInteger.ONE);
        trs.execute(input, COST);
        assertTrue(accountIsValid(trs, contract, acct));
    }

    @Test
    public void testAccountIsValidAfterMultipleDeposits() {
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);
        TRSuseContract trs = newTRSuseContract(acct);
        byte[] input = getDepositInput(contract, BigInteger.ONE);
        trs.execute(input, COST);
        trs.execute(input, COST);
        trs.execute(input, COST);
        trs.execute(input, COST);
        assertTrue(accountIsValid(trs, contract, acct));
    }

    @Test
    public void testMultipleAccountsValidAfterDeposits() {
        Address acct1 = getNewExistentAccount(DEFAULT_BALANCE);
        Address acct2 = getNewExistentAccount(DEFAULT_BALANCE);
        Address acct3 = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct1, false, true, 1,
            BigInteger.ZERO, 0);
        byte[] input = getDepositInput(contract, BigInteger.ONE);

        TRSuseContract trs1 = newTRSuseContract(acct1);
        trs1.execute(input, COST);
        TRSuseContract trs2 = newTRSuseContract(acct2);
        trs2.execute(input, COST);
        TRSuseContract trs3 = newTRSuseContract(acct3);
        trs3.execute(input, COST);

        assertTrue(accountIsValid(trs1, contract, acct1));
        assertTrue(accountIsValid(trs2, contract, acct2));
        assertTrue(accountIsValid(trs3, contract, acct3));
    }

    /*
     * We have an account "come" (deposit) and then "go" (refund all) and then come back again.
     * We want to ensure that the account's is-valid bit, its balance and the linked list are
     * all responding as expected to this.
     * First we test when we have only 1 user and then with multiple users.
    */

    @Test
    public void testAccountComingAndGoingSolo() {
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);

        // Come.
        TRSuseContract trs = newTRSuseContract(acct);
        byte[] input = getDepositInput(contract, DEFAULT_BALANCE);
        trs.execute(input, COST);

        assertTrue(accountIsValid(trs, contract, acct));
        assertTrue(accountIsEligibleForSpecial(trs, contract, acct));
        assertEquals(DEFAULT_BALANCE, getTotalBalance(trs, contract));
        assertEquals(DEFAULT_BALANCE, getDepositBalance(trs, contract, acct));
        assertEquals(acct, getLinkedListHead(trs, contract));
        assertNull(getLinkedListPrev(trs, contract, acct));
        assertNull(getLinkedListNext(trs, contract, acct));

        // Go.
        input = getRefundInput(contract, acct, DEFAULT_BALANCE);
        trs.execute(input, COST);

        assertFalse(accountIsValid(trs, contract, acct));
        assertFalse(accountIsEligibleForSpecial(trs, contract, acct));
        assertEquals(BigInteger.ZERO, getTotalBalance(trs, contract));
        assertEquals(BigInteger.ZERO, getDepositBalance(trs, contract, acct));
        assertNull(getLinkedListHead(trs, contract));
        assertNull(getLinkedListPrev(trs, contract, acct));
        assertNull(getLinkedListNext(trs, contract, acct));

        // Come back.
        BigInteger amt =  DEFAULT_BALANCE.subtract(BigInteger.ONE);
        input = getDepositInput(contract, amt);
        trs.execute(input, COST);

        assertTrue(accountIsValid(trs, contract, acct));
        assertTrue(accountIsEligibleForSpecial(trs, contract, acct));
        assertEquals(amt, getTotalBalance(trs, contract));
        assertEquals(amt, getDepositBalance(trs, contract, acct));
        assertEquals(acct, getLinkedListHead(trs, contract));
        assertNull(getLinkedListPrev(trs, contract, acct));
        assertNull(getLinkedListNext(trs, contract, acct));
    }

    @Test
    public void testAccountComingAndGoingMultipleUsers() {
        Address owner = getNewExistentAccount(BigInteger.ONE);
        Address acct1 = getNewExistentAccount(DEFAULT_BALANCE);
        Address acct2 = getNewExistentAccount(DEFAULT_BALANCE);
        Address acct3 = getNewExistentAccount(DEFAULT_BALANCE);
        Address acct4 = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(owner, false, true, 1,
            BigInteger.ZERO, 0);
        TRSuseContract trs = newTRSuseContract(owner);

        // Come.    We have:    head-> acct4 <-> acct3 <-> acct2 <-> acct1 -> null
        byte[] input = getDepositInput(contract, DEFAULT_BALANCE);
        newTRSuseContract(acct1).execute(input, COST);
        newTRSuseContract(acct2).execute(input, COST);
        newTRSuseContract(acct3).execute(input, COST);
        newTRSuseContract(acct4).execute(input, COST);

        assertTrue(accountIsValid(trs, contract, acct1));
        assertTrue(accountIsEligibleForSpecial(trs, contract, acct1));
        assertEquals(DEFAULT_BALANCE, getDepositBalance(trs, contract, acct1));
        assertTrue(accountIsValid(trs, contract, acct2));
        assertTrue(accountIsEligibleForSpecial(trs, contract, acct2));
        assertEquals(DEFAULT_BALANCE, getDepositBalance(trs, contract, acct2));
        assertTrue(accountIsValid(trs, contract, acct3));
        assertTrue(accountIsEligibleForSpecial(trs, contract, acct3));
        assertEquals(DEFAULT_BALANCE, getDepositBalance(trs, contract, acct3));
        assertTrue(accountIsValid(trs, contract, acct4));
        assertTrue(accountIsEligibleForSpecial(trs, contract, acct4));
        assertEquals(DEFAULT_BALANCE, getDepositBalance(trs, contract, acct4));
        assertEquals(DEFAULT_BALANCE.multiply(new BigInteger("4")), getTotalBalance(trs, contract));
        assertEquals(acct4, getLinkedListHead(trs, contract));
        assertEquals(acct3, getLinkedListNext(trs, contract, acct4));
        assertEquals(acct2, getLinkedListNext(trs, contract, acct3));
        assertEquals(acct1, getLinkedListNext(trs, contract, acct2));
        assertNull(getLinkedListNext(trs, contract, acct1));
        assertEquals(acct2, getLinkedListPrev(trs, contract, acct1));
        assertEquals(acct3, getLinkedListPrev(trs, contract, acct2));
        assertEquals(acct4, getLinkedListPrev(trs, contract, acct3));
        assertNull(getLinkedListPrev(trs, contract, acct4));

        // Go.  We have:    head-> acct3 <-> acct1 -> null
        input = getRefundInput(contract, acct2, DEFAULT_BALANCE);
        trs.execute(input, COST);
        input = getRefundInput(contract, acct4, DEFAULT_BALANCE);
        trs.execute(input, COST);

        assertTrue(accountIsValid(trs, contract, acct1));
        assertTrue(accountIsEligibleForSpecial(trs, contract, acct1));
        assertEquals(DEFAULT_BALANCE, getDepositBalance(trs, contract, acct1));
        assertFalse(accountIsValid(trs, contract, acct2));
        assertFalse(accountIsEligibleForSpecial(trs, contract, acct2));
        assertEquals(BigInteger.ZERO, getDepositBalance(trs, contract, acct2));
        assertTrue(accountIsValid(trs, contract, acct3));
        assertTrue(accountIsEligibleForSpecial(trs, contract, acct3));
        assertEquals(DEFAULT_BALANCE, getDepositBalance(trs, contract, acct3));
        assertFalse(accountIsValid(trs, contract, acct4));
        assertFalse(accountIsEligibleForSpecial(trs, contract, acct4));
        assertEquals(BigInteger.ZERO, getDepositBalance(trs, contract, acct4));
        assertEquals(DEFAULT_BALANCE.multiply(BigInteger.TWO), getTotalBalance(trs, contract));
        assertEquals(acct3, getLinkedListHead(trs, contract));
        assertEquals(acct1, getLinkedListNext(trs, contract, acct3));
        assertNull(getLinkedListNext(trs, contract, acct1));
        assertEquals(acct3, getLinkedListPrev(trs, contract, acct1));
        assertNull(getLinkedListPrev(trs, contract, acct3));

        // Come back. We have:  head-> acct2 <-> acct4 <-> acct3 <-> acct1 -> null
        input = getDepositInput(contract, DEFAULT_BALANCE);
        newTRSuseContract(acct4).execute(input, COST);
        newTRSuseContract(acct2).execute(input, COST);

        assertTrue(accountIsValid(trs, contract, acct1));
        assertTrue(accountIsEligibleForSpecial(trs, contract, acct1));
        assertEquals(DEFAULT_BALANCE, getDepositBalance(trs, contract, acct1));
        assertTrue(accountIsValid(trs, contract, acct2));
        assertTrue(accountIsEligibleForSpecial(trs, contract, acct2));
        assertEquals(DEFAULT_BALANCE, getDepositBalance(trs, contract, acct2));
        assertTrue(accountIsValid(trs, contract, acct3));
        assertTrue(accountIsEligibleForSpecial(trs, contract, acct3));
        assertEquals(DEFAULT_BALANCE, getDepositBalance(trs, contract, acct3));
        assertTrue(accountIsValid(trs, contract, acct4));
        assertTrue(accountIsEligibleForSpecial(trs, contract, acct4));
        assertEquals(DEFAULT_BALANCE, getDepositBalance(trs, contract, acct4));
        assertEquals(DEFAULT_BALANCE.multiply(new BigInteger("4")), getTotalBalance(trs, contract));
        assertEquals(acct2, getLinkedListHead(trs, contract));
        assertEquals(acct4, getLinkedListNext(trs, contract, acct2));
        assertEquals(acct3, getLinkedListNext(trs, contract, acct4));
        assertEquals(acct1, getLinkedListNext(trs, contract, acct3));
        assertNull(getLinkedListNext(trs, contract, acct1));
        assertEquals(acct3, getLinkedListPrev(trs, contract, acct1));
        assertEquals(acct4, getLinkedListPrev(trs, contract, acct3));
        assertEquals(acct2, getLinkedListPrev(trs, contract, acct4));
        assertNull(getLinkedListPrev(trs, contract, acct2));
    }

    // <----------------------------------TRS WITHDRAWAL TESTS------------------------------------->

    @Test
    public void testWithdrawInputTooShort() {
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);
        byte[] input = new byte[32];
        input[0] = 0x1;
        System.arraycopy(contract.toBytes(), 0, input, 1, Address.ADDRESS_LEN - 1);
        ContractExecutionResult res = newTRSuseContract(acct).execute(input, COST);
        assertEquals(ResultCode.INTERNAL_ERROR, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
    }

    @Test
    public void testWithdrawInputTooLong() {
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);
        byte[] input = new byte[34];
        input[0] = 0x1;
        System.arraycopy(contract.toBytes(), 0, input, 1, Address.ADDRESS_LEN);
        ContractExecutionResult res = newTRSuseContract(acct).execute(input, COST);
        assertEquals(ResultCode.INTERNAL_ERROR, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
    }

    @Test
    public void testWithdrawContractNotLockedOrLive() {
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);
        byte[] input = getDepositInput(contract, DEFAULT_BALANCE);
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(acct).execute(input, COST).getResultCode());

        input = getWithdrawInput(contract);
        ContractExecutionResult res = newTRSuseContract(acct).execute(input, COST);
        assertEquals(ResultCode.INTERNAL_ERROR, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
        assertEquals(DEFAULT_BALANCE, getDepositBalance(newTRSuseContract(acct), contract, acct));
    }

    @Test
    public void testWithdrawContractLockedNotLive() {
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);
        byte[] input = getDepositInput(contract, DEFAULT_BALANCE);
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(acct).execute(input, COST).getResultCode());

        // Lock the contract.
        input = getLockInput(contract);
        assertEquals(ResultCode.SUCCESS, newTRSstateContract(acct).execute(input, COST).getResultCode());

        input = getWithdrawInput(contract);
        ContractExecutionResult res = newTRSuseContract(acct).execute(input, COST);
        assertEquals(ResultCode.INTERNAL_ERROR, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
        assertEquals(DEFAULT_BALANCE, getDepositBalance(newTRSuseContract(acct), contract, acct));
    }

    @Test
    public void testLastWithdrawalPeriodNonExistentContract() {
        Address account = getNewExistentAccount(BigInteger.ONE);
        assertEquals(-1, getAccountLastWithdrawalPeriod(newTRSuseContract(account), account, account));
    }

    @Test
    public void testLastWithdrawalPeriodAccountNotInContract() {
        Address owner = getNewExistentAccount(BigInteger.ONE);
        Address stranger = getNewExistentAccount(BigInteger.ONE);
        Address contract = createTRScontract(owner, false, true, 1,
            BigInteger.ZERO, 0);

        assertEquals(-1, getAccountLastWithdrawalPeriod(newTRSuseContract(owner), contract, stranger));
    }

    @Test
    public void testLastWithdrawalPeriodBeforeLive() {
        // Test before locking.
        Address owner = getNewExistentAccount(BigInteger.ONE);
        Address acc = getNewExistentAccount(BigInteger.ONE);
        Address contract = createTRScontract(owner, false, true, 1,
            BigInteger.ZERO, 0);

        byte[] input = getDepositInput(contract, BigInteger.ONE);
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(acc).execute(input, COST).getResultCode());
        assertEquals(0, getAccountLastWithdrawalPeriod(newTRSuseContract(owner), contract, acc));

        // Test that locking changes nothing.
        input = getLockInput(contract);
        assertEquals(ResultCode.SUCCESS, newTRSstateContract(owner).execute(input, COST).getResultCode());
        assertEquals(0, getAccountLastWithdrawalPeriod(newTRSuseContract(owner), contract, acc));
    }

    @Test
    public void testLastWithdrawalPeriodOnceLive() {
        Address owner = getNewExistentAccount(BigInteger.ONE);
        Address acc = getNewExistentAccount(BigInteger.ONE);
        Address contract = createTRScontract(owner, false, true, 1,
            BigInteger.ZERO, 0);

        byte[] input = getDepositInput(contract, BigInteger.ONE);
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(acc).execute(input, COST).getResultCode());

        input = getLockInput(contract);
        assertEquals(ResultCode.SUCCESS, newTRSstateContract(owner).execute(input, COST).getResultCode());
        input = getStartInput(contract);
        assertEquals(ResultCode.SUCCESS, newTRSstateContract(owner).execute(input, COST).getResultCode());
        assertEquals(0, getAccountLastWithdrawalPeriod(newTRSuseContract(owner), contract, acc));
    }

    @Test
    public void testLastWithdrawalPeriodComingAndGoing() {
        Address owner = getNewExistentAccount(BigInteger.ONE);
        Address acc = getNewExistentAccount(BigInteger.ONE);
        Address contract = createTRScontract(owner, false, true, 1,
            BigInteger.ZERO, 0);

        byte[] input = getDepositInput(contract, BigInteger.ONE);
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(acc).execute(input, COST).getResultCode());
        assertEquals(0, getAccountLastWithdrawalPeriod(newTRSuseContract(owner), contract, acc));

        input = getRefundInput(contract, acc, BigInteger.ONE);
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(owner).execute(input, COST).getResultCode());
        assertEquals(-1, getAccountLastWithdrawalPeriod(newTRSuseContract(owner), contract, acc));

        input = getDepositInput(contract, BigInteger.ONE);
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(acc).execute(input, COST).getResultCode());
        assertEquals(0, getAccountLastWithdrawalPeriod(newTRSuseContract(owner), contract, acc));
    }

    @Test
    public void testWithdrawMultipleTimesSamePeriod() throws InterruptedException {
        int periods = DEFAULT_BALANCE.intValue();
        repo.addBalance(AION, DEFAULT_BALANCE);
        BigInteger initBal = repo.getBalance(AION);
        Address contract = createTRScontract(AION, true, true, periods,
            BigInteger.ZERO, 0);

        BigInteger expectedBalAfterDepo = initBal.subtract(DEFAULT_BALANCE);
        byte[] input = getDepositInput(contract, DEFAULT_BALANCE);
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(AION).execute(input, COST).getResultCode());
        assertEquals(expectedBalAfterDepo, repo.getBalance(AION));
        lockAndStartContract(contract, AION);
        createBlockchain(1, TimeUnit.SECONDS.toMillis(1));

        input = getWithdrawInput(contract);
        TRSuseContract trs = newTRSuseContract(AION);
        ContractExecutionResult res = trs.execute(input, COST);
        assertEquals(ResultCode.SUCCESS, res.getResultCode());
        assertEquals(0, res.getNrgLeft());

        BigInteger expectedAmt = expectedAmtFirstWithdraw(trs, contract, DEFAULT_BALANCE,
            DEFAULT_BALANCE, BigInteger.ZERO, BigDecimal.ZERO, periods);
        BigInteger expectedBal = expectedBalAfterDepo.add(expectedAmt);

        // Try to keep withdrawing...
        for (int i = 0; i < 5; i++) {
            assertEquals(ResultCode.INTERNAL_ERROR, trs.execute(input, COST).getResultCode());
            assertEquals(expectedBal, repo.getBalance(AION));
        }
    }

    @Test
    public void testWithdrawNoBonusFunds() throws InterruptedException {
        BigDecimal percent = new BigDecimal("10");
        BigInteger deposits = DEFAULT_BALANCE;
        BigInteger bonus = BigInteger.ZERO;
        int periods = 3;
        int numDepositors = 4;
        BigInteger total = deposits.multiply(BigInteger.valueOf(numDepositors));
        Address contract = setupContract(numDepositors, deposits, bonus, periods, percent);

        // We try to put the contract in a non-final period and withdraw.
        createBlockchain(1, TimeUnit.SECONDS.toMillis(1));
        AbstractTRS trs = newTRSstateContract(AION);
        BigInteger expectedAmt = expectedAmtFirstWithdraw(trs, contract, deposits, total, bonus, percent, periods);

        Set<Address> depositors = getAllDepositors(trs, contract);
        byte[] input = getWithdrawInput(contract);
        for (Address acc : depositors) {
            assertEquals(ResultCode.SUCCESS, newTRSuseContract(acc).execute(input, COST).getResultCode());
            assertEquals(expectedAmt, repo.getBalance(acc));
        }

        // Now put the contract in its final period and withdraw. All accounts should have their
        // origial balance back.
        addBlocks(1, TimeUnit.SECONDS.toMillis(3));
        assertEquals(periods, getContractCurrentPeriod(trs, contract));
        for (Address acc : depositors) {
            assertEquals(ResultCode.SUCCESS, newTRSuseContract(acc).execute(input, COST).getResultCode());
            assertEquals(deposits, repo.getBalance(acc));
        }
    }

    @Test
    public void testWithdrawSpecialOneOffOnFirstWithdraw() throws InterruptedException {
        BigDecimal percent = new BigDecimal("8");
        BigInteger deposits = new BigInteger("9283653985313");
        BigInteger bonus = new BigInteger("2386564");
        int periods = 3;
        int depositors = 5;
        BigInteger total = deposits.multiply(BigInteger.valueOf(depositors));
        Address contract = setupContract(depositors, deposits, bonus, periods, percent);

        // We try to put the contract in a non-final period and withdraw.
        createBlockchain(1, TimeUnit.SECONDS.toMillis(1));
        AbstractTRS trs = newTRSstateContract(AION);
        BigInteger expectedAmt = expectedAmtFirstWithdraw(trs, contract, deposits, total, bonus, percent, periods);

        Set<Address> contributors = getAllDepositors(trs, contract);
        byte[] input = getWithdrawInput(contract);
        for (Address acc : contributors) {
            assertEquals(ResultCode.SUCCESS, newTRSuseContract(acc).execute(input, COST).getResultCode());
            assertEquals(expectedAmt, repo.getBalance(acc));
        }
    }

    @Test
    public void testWithdrawSpecialEventBeforePeriodOne() throws InterruptedException {
        BigDecimal percent = new BigDecimal("72");
        BigInteger deposits = new BigInteger("7656234");
        BigInteger bonus = new BigInteger("92834756532");
        int periods = 2;
        int depositors = 3;
        BigInteger total = deposits.multiply(BigInteger.valueOf(depositors));
        Address contract = setupContract(depositors, deposits, bonus, periods, percent);

        // Only genesis block exists so contract is live but the first period hasn't begun because
        // there is no block that has been made after the contract went live. All depositors will
        // be eligible to withdraw only the special event amount.
        createBlockchain(0, 0);
        AbstractTRS trs = newTRSstateContract(AION);
        BigInteger expectedAmt = grabSpecialAmount(new BigDecimal(deposits), new BigDecimal(total),
            new BigDecimal(bonus), percent);

        Set<Address> contributors = getAllDepositors(trs, contract);
        byte[] input = getWithdrawInput(contract);
        for (Address acc : contributors) {
            assertEquals(ResultCode.SUCCESS, newTRSuseContract(acc).execute(input, COST).getResultCode());
            assertEquals(expectedAmt, repo.getBalance(acc));
        }

        // Let's try and withdraw again, now we should not be able to.
        for (Address acc : contributors) {
            assertEquals(ResultCode.INTERNAL_ERROR, newTRSuseContract(acc).execute(input, COST).getResultCode());
            assertEquals(expectedAmt, repo.getBalance(acc));
        }
    }

    @Test
    public void testWithdrawSomePeriodsBehind() throws InterruptedException {
        BigDecimal percent = new BigDecimal("25");
        BigInteger deposits = new BigInteger("384276532");
        BigInteger bonus = new BigInteger("9278");
        int periods = 7;
        int depositors = 5;
        BigInteger total = deposits.multiply(BigInteger.valueOf(depositors));
        Address contract = setupContract(depositors, deposits, bonus, periods, percent);

        // We try to put the contract in a non-final period greater than period 1 and withdraw.
        createBlockchain(3, TimeUnit.SECONDS.toMillis(1));
        AbstractTRS trs = newTRSstateContract(AION);
        assertTrue(grabCurrentPeriod(trs, contract).compareTo(BigInteger.ONE) > 0);
        BigInteger expectedAmt = expectedAmtFirstWithdraw(trs, contract, deposits, total, bonus, percent, periods);

        Set<Address> contributors = getAllDepositors(trs, contract);
        byte[] input = getWithdrawInput(contract);
        for (Address acc : contributors) {
            assertEquals(ResultCode.SUCCESS, newTRSuseContract(acc).execute(input, COST).getResultCode());
            assertEquals(expectedAmt, repo.getBalance(acc));
        }
    }

    @Test
    public void testWithdrawMultipleTimesInFinalPeriod() throws InterruptedException {
        BigDecimal percent = new BigDecimal("41.234856");
        BigInteger deposits = new BigInteger("394876");
        BigInteger bonus = new BigInteger("329487682345");
        int periods = 3;
        int depositors = 5;
        BigInteger total = deposits.multiply(BigInteger.valueOf(depositors));
        Address contract = setupContract(depositors, deposits, bonus, periods, percent);

        // We try to put the contract in a final period and withdraw.
        createBlockchain(1, TimeUnit.SECONDS.toMillis(4));
        AbstractTRS trs = newTRSstateContract(AION);
        assertEquals(BigInteger.valueOf(periods), grabCurrentPeriod(trs, contract));
        BigInteger owings = grabOwings(new BigDecimal(deposits), new BigDecimal(total), new BigDecimal(bonus));

        Set<Address> contributors = getAllDepositors(trs, contract);
        byte[] input = getWithdrawInput(contract);
        for (Address acc : contributors) {
            assertEquals(ResultCode.SUCCESS, newTRSuseContract(acc).execute(input, COST).getResultCode());
            assertEquals(owings, repo.getBalance(acc));
        }

        // Try to withdraw again from the final period.
        for (Address acc : contributors) {
            assertEquals(ResultCode.INTERNAL_ERROR, newTRSuseContract(acc).execute(input, COST).getResultCode());
            assertEquals(owings, repo.getBalance(acc));
        }
    }

    @Test
    public void testWithdrawOncePerBlockTotalOwings() throws InterruptedException {
        BigDecimal percent = new BigDecimal("0.000001");
        BigInteger deposits = new BigInteger("384276532");
        BigInteger bonus = new BigInteger("922355678");
        int periods = 4;
        int depositors = 8;
        BigInteger total = deposits.multiply(BigInteger.valueOf(depositors));
        Address contract = setupContract(depositors, deposits, bonus, periods, percent);

        AbstractTRS trs = newTRSstateContract(AION);
        createBlockchain(0, 0);

        boolean isAccNotDone;
        boolean isDone = false;
        byte[] input = getWithdrawInput(contract);
        while (!isDone) {
            isAccNotDone = false;

            Set<Address> contributors = getAllDepositors(trs, contract);
            for (Address acc : contributors) {
                if (newTRSuseContract(acc).execute(input, COST).getResultCode().equals(ResultCode.SUCCESS)) {
                    isAccNotDone = true;
                }
            }

            addBlocks(1, TimeUnit.SECONDS.toMillis(1));
            if (!isAccNotDone) { isDone = true; }
        }

        // Each account should have its total owings by now.
        BigInteger owings = grabOwings(new BigDecimal(deposits), new BigDecimal(total), new BigDecimal(bonus));

        BigInteger sum = BigInteger.ZERO;
        Set<Address> contributors = getAllDepositors(trs, contract);
        for (Address acc : contributors) {
            assertEquals(owings, repo.getBalance(acc));
            sum = sum.add(owings);
        }

        // Ensure the contract does not pay out more than it has and that the lower bound on the remainder
        // is n-1 for n depositors.
        BigInteger totalFunds = total.add(bonus);
        assertTrue(sum.compareTo(totalFunds) <= 0);
        assertTrue(sum.compareTo(totalFunds.subtract(BigInteger.valueOf(depositors - 1))) >= 0);
    }

    @Test
    public void testWithdrawLastPeriodOnlyTotalOwings() throws InterruptedException {
        BigDecimal percent = new BigDecimal("13.522");
        BigInteger deposits = new BigInteger("384276436375532");
        BigInteger bonus = new BigInteger("92783242");
        int periods = 3;
        int depositors = 2;
        BigInteger total = deposits.multiply(BigInteger.valueOf(depositors));
        Address contract = setupContract(depositors, deposits, bonus, periods, percent);

        // We try to put the contract in the final period and withdraw.
        createBlockchain(1, TimeUnit.SECONDS.toMillis(4));
        AbstractTRS trs = newTRSstateContract(AION);

        // We are in last period so we expect to withdraw our total owings.
        BigInteger currPeriod = grabCurrentPeriod(trs, contract);
        assertEquals(BigInteger.valueOf(periods), currPeriod);
        BigInteger accOwed = grabOwings(new BigDecimal(deposits), new BigDecimal(total), new BigDecimal(bonus));

        Set<Address> contributors = getAllDepositors(trs, contract);
        byte[] input = getWithdrawInput(contract);
        for (Address acc : contributors) {
            assertEquals(ResultCode.SUCCESS, newTRSuseContract(acc).execute(input, COST).getResultCode());
            assertEquals(accOwed, repo.getBalance(acc));
        }
    }

    @Test
    public void testWithdrawSomePeriodsTotalOwings() throws InterruptedException {
        BigDecimal percent = new BigDecimal("13.33333112");
        BigInteger deposits = new BigInteger("3842762");
        BigInteger bonus = new BigInteger("9223247118");
        int periods = 5;
        int depositors = 4;
        BigInteger total = deposits.multiply(BigInteger.valueOf(depositors));
        Address contract = setupContract(depositors, deposits, bonus, periods, percent);

        AbstractTRS trs = newTRSstateContract(AION);
        createBlockchain(0, 0);

        boolean isAccNotDone;
        boolean isDone = false;
        while (!isDone) {
            isAccNotDone = false;

            Set<Address> contributors = getAllDepositors(trs, contract);
            byte[] input = getWithdrawInput(contract);
            for (Address acc : contributors) {
                if (newTRSuseContract(acc).execute(input, COST).getResultCode().equals(ResultCode.SUCCESS)) {
                    isAccNotDone = true;
                }
            }

            addBlocks(1, TimeUnit.SECONDS.toMillis(2));
            if (!isAccNotDone) { isDone = true; }
        }

        // Each account should have its total owings by now.
        BigInteger owings = grabOwings(new BigDecimal(deposits), new BigDecimal(total), new BigDecimal(bonus));

        BigInteger sum = BigInteger.ZERO;
        Set<Address> contributors = getAllDepositors(trs, contract);
        for (Address acc : contributors) {
            assertEquals(owings, repo.getBalance(acc));
            sum = sum.add(owings);
        }

        // Ensure the contract does not pay out more than it has and that the lower bound on the remainder
        // is n-1 for n depositors.
        BigInteger totalFunds = total.add(bonus);
        assertTrue(sum.compareTo(totalFunds) <= 0);
        assertTrue(sum.compareTo(totalFunds.subtract(BigInteger.valueOf(depositors - 1))) >= 0);
    }

    @Test
    public void testWithdrawLargeWealthGap() throws InterruptedException {
        BigInteger bal1 = BigInteger.ONE;
        BigInteger bal2 = new BigInteger("968523984325");
        BigInteger bal3 = new BigInteger("129387461289371");
        Address acc1 = getNewExistentAccount(bal1);
        Address acc2 = getNewExistentAccount(bal2);
        Address acc3 = getNewExistentAccount(bal3);
        Address contract = createTRScontract(AION, true, true, 4,
            BigInteger.ZERO, 0);

        byte[] input = getDepositInput(contract, bal1);
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(acc1).execute(input, COST).getResultCode());
        input = getDepositInput(contract, bal2);
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(acc2).execute(input, COST).getResultCode());
        input = getDepositInput(contract, bal3);
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(acc3).execute(input, COST).getResultCode());
        BigInteger bonus = new BigInteger("9238436745867623");
        repo.addBalance(contract, bonus);
        lockAndStartContract(contract, AION);

        AbstractTRS trs = newTRSstateContract(AION);
        createBlockchain(0, 0);

        boolean isDone = false;
        input = getWithdrawInput(contract);
        while (!isDone) {

            Set<Address> contributors = getAllDepositors(trs, contract);
            for (Address acc : contributors) {
                newTRSuseContract(acc).execute(input, COST);
            }

            if (grabCurrentPeriod(trs, contract).intValue() == 4) { isDone = true; }
            addBlocks(1, TimeUnit.SECONDS.toMillis(1));
        }

        // We should have more than 1 owings back because bonus is large enough.
        BigInteger total = bal1.add(bal2.add(bal3));
        BigInteger owings = grabOwings(new BigDecimal(bal1), new BigDecimal(total), new BigDecimal(bonus));
        assertTrue(owings.compareTo(BigInteger.ONE) > 0);
        assertEquals(owings, repo.getBalance(acc1));

        // Finally verify the other owings and their sum as well.
        BigInteger owings2 = grabOwings(new BigDecimal(bal2), new BigDecimal(total), new BigDecimal(bonus));
        BigInteger owings3 = grabOwings(new BigDecimal(bal3), new BigDecimal(total), new BigDecimal(bonus));
        BigInteger sum = owings.add(owings2).add(owings3);
        assertTrue(sum.compareTo(total.add(bonus)) <= 0);
        assertTrue(sum.compareTo(total.add(bonus).subtract(BigInteger.TWO)) <= 0);
    }

    @Test
    public void testWithdrawOneDepositorTotalOwings() throws InterruptedException {
        BigDecimal percent = new BigDecimal("61.13");
        BigInteger deposits = new BigInteger("435346542677");
        BigInteger bonus = new BigInteger("326543");
        int periods = 3;
        int depositors = 1;
        BigInteger total = deposits.multiply(BigInteger.valueOf(depositors));
        Address contract = setupContract(depositors, deposits, bonus, periods, percent);

        // We try to put the contract in a non-final period greater than period 1 and withdraw.
        createBlockchain(4, TimeUnit.SECONDS.toMillis(1));
        AbstractTRS trs = newTRSstateContract(AION);
        assertEquals(BigInteger.valueOf(periods), grabCurrentPeriod(trs, contract));
        BigInteger owings = grabOwings(new BigDecimal(deposits), new BigDecimal(total), new BigDecimal(bonus));

        Set<Address> contributors = getAllDepositors(trs, contract);
        byte[] input = getWithdrawInput(contract);
        for (Address acc : contributors) {
            assertEquals(ResultCode.SUCCESS, newTRSuseContract(acc).execute(input, COST).getResultCode());
            assertEquals(owings, repo.getBalance(acc));
        }
    }

    @Test
    public void testWithdrawSmallDepositsLargeBonus() throws InterruptedException {
        BigDecimal percent = new BigDecimal("17.000012");
        BigInteger deposits = BigInteger.ONE;
        BigInteger bonus = new BigInteger("8").multiply(new BigInteger("962357283486"));
        int periods = 4;
        int depositors = 8;
        BigInteger total = deposits.multiply(BigInteger.valueOf(depositors));
        Address contract = setupContract(depositors, deposits, bonus, periods, percent);

        // We try to put the contract in a non-final period and withdraw.
        AbstractTRS trs = newTRSstateContract(AION);
        createBlockchain(1, TimeUnit.SECONDS.toMillis(1));
        assertTrue(grabCurrentPeriod(trs, contract).compareTo(BigInteger.valueOf(periods)) < 0);

        // The bonus is divisible by n depositors so we know the depositors will get back bonus/8 + 1.
        BigInteger owings = (bonus.divide(new BigInteger("8"))).add(BigInteger.ONE);
        BigInteger amt = expectedAmtFirstWithdraw(trs, contract, deposits, total, bonus, percent, periods);

        Set<Address> contributors = getAllDepositors(trs, contract);
        byte[] input = getWithdrawInput(contract);
        for (Address acc : contributors) {
            assertEquals(ResultCode.SUCCESS, newTRSuseContract(acc).execute(input, COST).getResultCode());
            assertEquals(amt, repo.getBalance(acc));
        }

        // No put contract in final period and withdraw.
        createBlockchain(1, TimeUnit.SECONDS.toMillis(5));
        assertEquals(BigInteger.valueOf(periods), grabCurrentPeriod(trs, contract));
        for (Address acc : contributors) {
            assertEquals(ResultCode.SUCCESS, newTRSuseContract(acc).execute(input, COST).getResultCode());
            assertEquals(owings, repo.getBalance(acc));
        }
    }

    @Test
    public void testWithdrawSpecialEventIsAllFunds() throws InterruptedException {
        BigDecimal percent = new BigDecimal("100");
        BigInteger deposits = new BigInteger("23425");
        BigInteger bonus = new BigInteger("92351074341");
        int periods = 7;
        int depositors = 6;
        BigInteger total = deposits.multiply(BigInteger.valueOf(depositors));
        Address contract = setupContract(depositors, deposits, bonus, periods, percent);

        // No blocks, so only special withdrawal is available. Since special is 100% we claim our
        // total owings right here.
        createBlockchain(0,0);
        AbstractTRS trs = newTRSstateContract(AION);
        assertEquals(0, grabCurrentPeriod(trs, contract).intValue());
        BigInteger owings = grabOwings(new BigDecimal(deposits), new BigDecimal(total), new BigDecimal(bonus));

        Set<Address> contributors = getAllDepositors(trs, contract);
        byte[] input = getWithdrawInput(contract);
        for (Address acc : contributors) {
            assertEquals(ResultCode.SUCCESS, newTRSuseContract(acc).execute(input, COST).getResultCode());
            assertEquals(owings, repo.getBalance(acc));
        }

        // Now move into a non-final period and ensure no more withdrawals can be made.
        addBlocks(1, TimeUnit.SECONDS.toMillis(1));
        for (Address acc : contributors) {
            assertEquals(ResultCode.INTERNAL_ERROR, newTRSuseContract(acc).execute(input, COST).getResultCode());
            assertEquals(owings, repo.getBalance(acc));
        }

        // Now move into a final period and ensure no more withdrawals can be made.
        addBlocks(1, TimeUnit.SECONDS.toMillis(7));
        assertEquals(BigInteger.valueOf(periods), grabCurrentPeriod(trs, contract));
        for (Address acc : contributors) {
            assertEquals(ResultCode.INTERNAL_ERROR, newTRSuseContract(acc).execute(input, COST).getResultCode());
            assertEquals(owings, repo.getBalance(acc));
        }
    }

    @Test
    public void testWithdrawSpecialEventVeryLarge() throws InterruptedException {
        BigDecimal percent = new BigDecimal("99.999");
        BigInteger deposits = new BigInteger("2500");
        BigInteger bonus = new BigInteger("10000");
        int periods = 6;
        int depositors = 4;
        BigInteger total = deposits.multiply(BigInteger.valueOf(depositors));
        Address contract = setupContract(depositors, deposits, bonus, periods, percent);

        // No blocks, so only special withdrawal is available. The amount leftover from the special
        // is in the range (0,1) and so all subsequent withdrawal periods should withdraw zero until
        // the final period, where the 1 token is finally claimed.
        createBlockchain(0,0);
        AbstractTRS trs = newTRSstateContract(AION);
        assertEquals(0, grabCurrentPeriod(trs, contract).intValue());
        BigInteger amt = expectedAmtFirstWithdraw(trs, contract, deposits, total, bonus, percent, periods);
        BigInteger owings = grabOwings(new BigDecimal(deposits), new BigDecimal(total), new BigDecimal(bonus));
        assertTrue(amt.compareTo(owings) < 0);

        Set<Address> contributors = getAllDepositors(trs, contract);
        byte[] input = getWithdrawInput(contract);
        for (Address acc : contributors) {
            assertEquals(ResultCode.SUCCESS, newTRSuseContract(acc).execute(input, COST).getResultCode());
            assertEquals(amt, repo.getBalance(acc));
        }

        // Now move into a non-final period and ensure all withdrawals fail (no positive amount to claim).
        addBlocks(1, TimeUnit.SECONDS.toMillis(1));
        for (Address acc : contributors) {
            assertEquals(ResultCode.INTERNAL_ERROR, newTRSuseContract(acc).execute(input, COST).getResultCode());
            assertEquals(amt, repo.getBalance(acc));
        }

        // Now move into a final period and ensure we can make a withdrawal for our last coin.
        addBlocks(1, TimeUnit.SECONDS.toMillis(7));
        assertEquals(BigInteger.valueOf(periods), grabCurrentPeriod(trs, contract));
        for (Address acc : contributors) {
            assertEquals(ResultCode.SUCCESS, newTRSuseContract(acc).execute(input, COST).getResultCode());
            assertEquals(owings, repo.getBalance(acc));
        }
    }

    @Test
    public void testWithdrawMakeBonusDepositsAfterIsLive() throws InterruptedException {
        BigDecimal percent = new BigDecimal("10");
        BigInteger deposits = new BigInteger("2500");
        BigInteger bonus = BigInteger.ZERO;
        int periods = 2;
        int depositors = 3;
        Address contract = setupContract(depositors, deposits, bonus, periods, percent);

        // Make a large bonus deposit while contract is live.
        repo.addBalance(contract, new BigInteger("876523876532534634"));

        // Withdraw all funds and ensure it equals deposits, meaning the later bonus was ignored.
        createBlockchain(1, TimeUnit.SECONDS.toMillis(3));
        byte[] input = getWithdrawInput(contract);
        AbstractTRS trs = newTRSstateContract(AION);
        Set<Address> contributors = getAllDepositors(trs, contract);
        for (Address acc : contributors) {
            assertEquals(ResultCode.SUCCESS, newTRSuseContract(acc).execute(input, COST).getResultCode());
            assertEquals(deposits, repo.getBalance(acc));
        }
    }

    @Test
    public void testWithdrawContractHasOnePeriod() throws InterruptedException {
        BigDecimal percent = new BigDecimal("10.1");
        BigInteger deposits = new BigInteger("2500");
        BigInteger bonus = new BigInteger("1000");
        int periods = 1;
        int depositors = 3;
        BigInteger total = deposits.multiply(BigInteger.valueOf(depositors));
        Address contract = setupContract(depositors, deposits, bonus, periods, percent);

        BigInteger owings = grabOwings(new BigDecimal(deposits), new BigDecimal(total), new BigDecimal(bonus));

        // Move into final period and withdraw.
        createBlockchain(1, 0);
        byte[] input = getWithdrawInput(contract);
        AbstractTRS trs = newTRSstateContract(AION);
        Set<Address> contributors = getAllDepositors(trs, contract);
        for (Address acc : contributors) {
            assertEquals(ResultCode.SUCCESS, newTRSuseContract(acc).execute(input, COST).getResultCode());
            assertEquals(owings, repo.getBalance(acc));
        }
    }

    @Test
    public void testWithdrawSpecialPercentage18DecimalsPrecise() throws InterruptedException {
        BigDecimal percent = new BigDecimal("0.000000000000000001");
        BigInteger deposits = new BigInteger("100000000000000000000");
        BigInteger bonus = BigInteger.ZERO;
        int periods = 3;
        int depositors = 3;
        BigInteger total = deposits.multiply(BigInteger.valueOf(depositors));
        Address contract = setupContract(depositors, deposits, bonus, periods, percent);

        // Move into non-final period and withdraw.
        createBlockchain(1, 0);

        AbstractTRS trs = newTRSstateContract(AION);
        BigInteger amt = expectedAmtFirstWithdraw(trs, contract, deposits, total, bonus, percent, periods);
        assertTrue(grabCurrentPeriod(trs, contract).compareTo(BigInteger.valueOf(periods)) < 0);

        BigInteger owings = grabOwings(new BigDecimal(deposits), new BigDecimal(total), new BigDecimal(bonus));
        BigInteger spec = grabSpecialAmount(new BigDecimal(deposits), new BigDecimal(total), new BigDecimal(bonus), percent);
        BigInteger rawAmt = grabWithdrawAmt(owings, spec, periods);
        assertEquals(BigInteger.ONE, spec);
        assertEquals(amt, rawAmt.add(spec));

        Set<Address> contributors = getAllDepositors(trs, contract);
        byte[] input = getWithdrawInput(contract);

        boolean firstLook = true;
        boolean isAccNotDone;
        boolean isDone = false;
        while (!isDone) {
            isAccNotDone = false;

            for (Address acc : contributors) {
                if (newTRSuseContract(acc).execute(input, COST).getResultCode().equals(ResultCode.SUCCESS)) {
                    isAccNotDone = true;
                }
                if ((firstLook) && (grabCurrentPeriod(trs, contract).intValue() == 1)) {
                    assertEquals(amt, repo.getBalance(acc));
                }
            }

            addBlocks(1, TimeUnit.SECONDS.toMillis(1));
            firstLook = false;
            if (!isAccNotDone) { isDone = true; }
        }

        for (Address acc : contributors) {
            assertEquals(owings, repo.getBalance(acc));
        }
    }

    // <-------------------------------TRS BULK-WITHDRAWAL TESTS----------------------------------->

    @Test
    public void testBulkWithdrawInputTooShort() {
        // Test maximum too-short size.
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);
        byte[] input = new byte[32];
        input[0] = 0x3;
        System.arraycopy(contract.toBytes(), 0, input, 1, Address.ADDRESS_LEN - 1);
        ContractExecutionResult res = newTRSuseContract(acct).execute(input, COST);
        assertEquals(ResultCode.INTERNAL_ERROR, res.getResultCode());
        assertEquals(0, res.getNrgLeft());

        // Test minimum too-short size.
        input = new byte[1];
        res = newTRSuseContract(acct).execute(input, COST);
        assertEquals(ResultCode.INTERNAL_ERROR, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
    }

    @Test
    public void testBulkWithdrawInputTooLong() {
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);
        byte[] input = new byte[34];
        input[0] = 0x3;
        System.arraycopy(contract.toBytes(), 0, input, 1, Address.ADDRESS_LEN);
        ContractExecutionResult res = newTRSuseContract(acct).execute(input, COST);
        assertEquals(ResultCode.INTERNAL_ERROR, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
    }

    @Test
    public void testBulkWithdrawCallerNotOwner() {
        BigDecimal percent = new BigDecimal("44.44");
        BigInteger deposits = new BigInteger("2652545");
        BigInteger bonus = new BigInteger("326543");
        int periods = 3;
        int depositors = 6;
        Address contract = setupContract(depositors, deposits, bonus, periods, percent);

        // Try to do a bulk-withdraw calling as every depositor in the contract (owner is AION).
        byte[] input = getBulkWithdrawInput(contract);
        AbstractTRS trs = newTRSstateContract(AION);
        Set<Address> contributors = getAllDepositors(trs, contract);
        for (Address acc : contributors) {
            assertEquals(ResultCode.INTERNAL_ERROR, newTRSuseContract(acc).execute(input, COST).getResultCode());
        }

        // Verify no one received any funds.
        for (Address acc : contributors) {
            assertEquals(BigInteger.ZERO, repo.getBalance(acc));
        }
    }

    @Test
    public void testBulkWithdrawContractNotLockedNotLive() {
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);

        // Try first with no one in the contract. Caller is owner.
        byte[] input = getBulkWithdrawInput(contract);
        assertEquals(ResultCode.INTERNAL_ERROR, newTRSuseContract(acct).execute(input, COST).getResultCode());

        // Now deposit some and try again.
        input = getDepositInput(contract, DEFAULT_BALANCE);
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(acct).execute(input, COST).getResultCode());
        input = getBulkWithdrawInput(contract);
        assertEquals(ResultCode.INTERNAL_ERROR, newTRSuseContract(acct).execute(input, COST).getResultCode());
    }

    @Test
    public void testBulkWithdrawContractLockedNotLive() {
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);

        // Deposit some funds and lock.
        byte[] input = getDepositInput(contract, DEFAULT_BALANCE);
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(acct).execute(input, COST).getResultCode());
        input = getLockInput(contract);
        assertEquals(ResultCode.SUCCESS, newTRSstateContract(acct).execute(input, COST).getResultCode());

        // Try to withdraw.
        input = getBulkWithdrawInput(contract);
        assertEquals(ResultCode.INTERNAL_ERROR, newTRSuseContract(acct).execute(input, COST).getResultCode());
        assertEquals(BigInteger.ZERO, repo.getBalance(acct));
    }

    @Test
    public void testBulkWithdrawOneDepositor() throws InterruptedException {
        BigDecimal percent = new BigDecimal("12.008");
        BigInteger deposits = new BigInteger("11118943432");
        BigInteger bonus = new BigInteger("346");
        int periods = 3;
        int depositors = 1;
        Address contract = setupContract(depositors, deposits, bonus, periods, percent);

        // Verify depositor has no account balance.
        AbstractTRS trs = newTRSstateContract(AION);
        Address depositor = getAllDepositors(trs, contract).iterator().next();
        assertEquals(BigInteger.ZERO, repo.getBalance(depositor));

        createBlockchain(1, TimeUnit.SECONDS.toMillis(1));

        // Do a bulk-withdraw from the contract.
        byte[] input = getBulkWithdrawInput(contract);
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(AION).execute(input, COST).getResultCode());
        BigInteger amt = expectedAmtFirstWithdraw(trs, contract, deposits, deposits, bonus, percent, periods);
        assertEquals(amt, repo.getBalance(depositor));
    }

    @Test
    public void testBulkWithdrawMultipleDepositors() throws InterruptedException {
        BigDecimal percent = new BigDecimal("61.987653");
        BigInteger deposits = new BigInteger("346264344");
        BigInteger bonus = new BigInteger("18946896534");
        int periods = 3;
        int depositors = 6;
        Address contract = setupContract(depositors, deposits, bonus, periods, percent);

        AbstractTRS trs = newTRSstateContract(AION);
        createBlockchain(1, TimeUnit.SECONDS.toMillis(1));
        BigInteger total = deposits.multiply(BigInteger.valueOf(depositors));
        BigInteger amt = expectedAmtFirstWithdraw(trs, contract, deposits, total, bonus, percent, periods);

        // Do a bulk-withdraw on the contract.
        byte[] input = getBulkWithdrawInput(contract);
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(AION).execute(input, COST).getResultCode());

        Set<Address> contributors = getAllDepositors(trs, contract);
        for (Address acc : contributors) {
            assertEquals(amt, repo.getBalance(acc));
        }

        // Move into final period and withdraw the rest.
        addBlocks(1, TimeUnit.SECONDS.toMillis(4));
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(AION).execute(input, COST).getResultCode());
        BigInteger owings = grabOwings(new BigDecimal(deposits), new BigDecimal(total), new BigDecimal(bonus));
        for (Address acc : contributors) {
            assertEquals(owings, repo.getBalance(acc));
        }
    }

    @Test
    public void testBulkWithdrawSomeDepositorsHaveWithdrawnSomeNotThisPeriod() throws InterruptedException {
        BigDecimal percent = new BigDecimal("74.237");
        BigInteger deposits = new BigInteger("3436");
        BigInteger bonus = new BigInteger("345");
        int periods = 3;
        int depositors = 5;
        Address contract = setupContract(depositors, deposits, bonus, periods, percent);

        AbstractTRS trs = newTRSstateContract(AION);
        createBlockchain(1, TimeUnit.SECONDS.toMillis(1));
        BigInteger total = deposits.multiply(BigInteger.valueOf(depositors));
        BigInteger amt = expectedAmtFirstWithdraw(trs, contract, deposits, total, bonus, percent, periods);

        // Have half of the depositors withdraw.
        byte[] input = getWithdrawInput(contract);
        boolean withdraw = true;
        Set<Address> contributors = getAllDepositors(trs, contract);
        for (Address acc : contributors) {
            if (withdraw) {
                assertEquals(ResultCode.SUCCESS, newTRSuseContract(acc).execute(input, COST).getResultCode());
                assertEquals(amt, repo.getBalance(acc));
            } else {
                assertEquals(BigInteger.ZERO, repo.getBalance(acc));
            }
            withdraw = !withdraw;
        }

        // Do a bulk-withdraw on the contract. Check all accounts have amt now and no one has more.
        input = getBulkWithdrawInput(contract);
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(AION).execute(input, COST).getResultCode());
        for (Address acc : contributors) {
            assertEquals(amt, repo.getBalance(acc));
        }
    }

    @Test
    public void testBulkWithdrawSpecialEventBeforeLiveBlockArrives() throws InterruptedException {
        BigDecimal percent = new BigDecimal("21");
        BigInteger deposits = new BigInteger("324876");
        BigInteger bonus = new BigInteger("3").pow(13);
        int periods = 4;
        int depositors = 3;
        Address contract = setupContract(depositors, deposits, bonus, periods, percent);

        // We have only a genesis block so by block timestamps we are not yet live.
        AbstractTRS trs = newTRSstateContract(AION);
        createBlockchain(0, 0);
        BigInteger total = deposits.multiply(BigInteger.valueOf(depositors));

        // Do a bulk-withdraw on the contract. Check all accounts have received only the special
        // withdrawal amount and nothing more.
        BigInteger spec = grabSpecialAmount(new BigDecimal(deposits), new BigDecimal(total), new BigDecimal(bonus), percent);

        Set<Address> contributors = getAllDepositors(trs, contract);
        byte[] input = getBulkWithdrawInput(contract);
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(AION).execute(input, COST).getResultCode());
        for (Address acc : contributors) {
            assertEquals(spec, repo.getBalance(acc));
        }
    }

    @Test
    public void testBulkWithdrawAtFinalPeriod() throws InterruptedException {
        BigDecimal percent = new BigDecimal("17.793267");
        BigInteger deposits = new BigInteger("96751856431");
        BigInteger bonus = new BigInteger("3274436");
        int periods = 4;
        int depositors = 12;
        Address contract = setupContract(depositors, deposits, bonus, periods, percent);

        // Move contract into its final period.
        AbstractTRS trs = newTRSstateContract(AION);
        createBlockchain(1, TimeUnit.SECONDS.toMillis(5));
        assertEquals(grabCurrentPeriod(trs, contract).intValue(), periods);

        // Verify each depositor gets their total owings back (and that the sum they collect is available)
        BigInteger total = deposits.multiply(BigInteger.valueOf(depositors));
        BigInteger owings = grabOwings(new BigDecimal(deposits), new BigDecimal(total), new BigDecimal(bonus));
        assertTrue((owings.multiply(BigInteger.valueOf(depositors))).compareTo(total.add(bonus)) <= 0);

        Set<Address> contributors = getAllDepositors(trs, contract);
        byte[] input = getBulkWithdrawInput(contract);
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(AION).execute(input, COST).getResultCode());
        for (Address acc : contributors) {
            assertEquals(owings, repo.getBalance(acc));
        }
    }

    @Test
    public void testBulkWithdrawMultipleTimesSpecialPeriod() throws InterruptedException {
        BigDecimal percent = new BigDecimal("21");
        BigInteger deposits = new BigInteger("324876");
        BigInteger bonus = new BigInteger("3").pow(13);
        int periods = 4;
        int depositors = 3;
        Address contract = setupContract(depositors, deposits, bonus, periods, percent);

        // We have only a genesis block so by block timestamps we are not yet live.
        AbstractTRS trs = newTRSstateContract(AION);
        createBlockchain(0, 0);
        BigInteger total = deposits.multiply(BigInteger.valueOf(depositors));

        // Do a bulk-withdraw on the contract. Check all accounts have received only the special
        // withdrawal amount and nothing more.
        BigInteger spec = grabSpecialAmount(new BigDecimal(deposits), new BigDecimal(total), new BigDecimal(bonus), percent);

        Set<Address> contributors = getAllDepositors(trs, contract);
        byte[] input = getBulkWithdrawInput(contract);
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(AION).execute(input, COST).getResultCode());
        for (Address acc : contributors) {
            assertEquals(spec, repo.getBalance(acc));
        }

        // Attempt to do another bulk withdraw in this same period; account balances should not change.
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(AION).execute(input, COST).getResultCode());
        for (Address acc : contributors) {
            assertEquals(spec, repo.getBalance(acc));
        }
    }

    @Test
    public void testBulkWithdrawMultipleTimesNonFinalPeriod() throws InterruptedException {
        BigDecimal percent = new BigDecimal("43");
        BigInteger deposits = new BigInteger("3744323");
        BigInteger bonus = new BigInteger("8347634");
        int periods = 6;
        int depositors = 8;
        Address contract = setupContract(depositors, deposits, bonus, periods, percent);

        // Move contract into a non-final period.
        AbstractTRS trs = newTRSstateContract(AION);
        createBlockchain(1, TimeUnit.SECONDS.toMillis(2));
        BigInteger total = deposits.multiply(BigInteger.valueOf(depositors));

        // Do a bulk-withdraw on the contract. Check all accounts have received amt.
        BigInteger amt = expectedAmtFirstWithdraw(trs, contract, deposits, total, bonus, percent, periods);

        Set<Address> contributors = getAllDepositors(trs, contract);
        byte[] input = getBulkWithdrawInput(contract);
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(AION).execute(input, COST).getResultCode());
        for (Address acc : contributors) {
            assertEquals(amt, repo.getBalance(acc));
        }

        // Attempt to do another bulk withdraw in this same period; account balances should not change.
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(AION).execute(input, COST).getResultCode());
        for (Address acc : contributors) {
            assertEquals(amt, repo.getBalance(acc));
        }
    }

    @Test
    public void testBulkWithdrawMultipleTimesFinalPeriod() throws InterruptedException {
        BigDecimal percent = new BigDecimal("2.0001");
        BigInteger deposits = new BigInteger("1000000000");
        BigInteger bonus = new BigInteger("650000");
        int periods = 2;
        int depositors = 11;
        Address contract = setupContract(depositors, deposits, bonus, periods, percent);

        // Move contract into a non-final period.
        AbstractTRS trs = newTRSstateContract(AION);
        createBlockchain(1, TimeUnit.SECONDS.toMillis(3));
        BigInteger total = deposits.multiply(BigInteger.valueOf(depositors));

        // Do a bulk-withdraw on the contract. Check all accounts have received amt.
        BigInteger owings = grabOwings(new BigDecimal(deposits), new BigDecimal(total), new BigDecimal(bonus));

        Set<Address> contributors = getAllDepositors(trs, contract);
        byte[] input = getBulkWithdrawInput(contract);
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(AION).execute(input, COST).getResultCode());
        for (Address acc : contributors) {
            assertEquals(owings, repo.getBalance(acc));
        }

        // Attempt to do another bulk withdraw in this same period; account balances should not change.
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(AION).execute(input, COST).getResultCode());
        for (Address acc : contributors) {
            assertEquals(owings, repo.getBalance(acc));
        }
    }

    // <--------------------------------------REFUND TRS TESTS------------------------------------->

    @Test
    public void testRefundInputTooShort() {
        // Test maximum too-short size.
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        byte[] input = new byte[192];
        ContractExecutionResult res = newTRSuseContract(acct).execute(input, COST);
        assertEquals(ResultCode.INTERNAL_ERROR, res.getResultCode());
        assertEquals(0, res.getNrgLeft());

        // Test minimum too-short size.
        input = new byte[1];
        res = newTRSuseContract(acct).execute(input, COST);
        assertEquals(ResultCode.INTERNAL_ERROR, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
    }

    @Test
    public void testRefundInputTooLarge() {
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        byte[] input = new byte[194];
        ContractExecutionResult res = newTRSuseContract(acct).execute(input, COST);
        assertEquals(ResultCode.INTERNAL_ERROR, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
    }

    @Test
    public void testRefundBadTRScontract() {
        // Test TRS address that looks like regular account address.
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = getNewExistentAccount(DEFAULT_BALANCE);
        byte[] input = getRefundInput(contract, acct, BigInteger.ZERO);
        ContractExecutionResult res = newTRSuseContract(acct).execute(input, COST);
        assertEquals(ResultCode.INTERNAL_ERROR, res.getResultCode());
        assertEquals(0, res.getNrgLeft());

        // Test TRS address with TRS prefix, so it looks legit.
        byte[] addr = ECKeyFac.inst().create().getAddress();
        addr[0] = (byte) 0xC0;
        contract = new Address(addr);
        tempAddrs.add(contract);
        input = getRefundInput(contract, acct, BigInteger.ZERO);
        res = newTRSuseContract(acct).execute(input, COST);
        assertEquals(ResultCode.INTERNAL_ERROR, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
    }

    @Test
    public void testRefundCallerIsNotOwner() {
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address acct2 = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);

        // acct2 deposits so that it does have a balance to refund from.
        byte[] input = getDepositInput(contract, BigInteger.ONE);
        TRSuseContract trs = newTRSuseContract(acct2);
        ContractExecutionResult res = trs.execute(input, COST);
        assertEquals(ResultCode.SUCCESS, res.getResultCode());

        // acct2 calls refund but owner is acct
        input = getRefundInput(contract, acct2, BigInteger.ONE);
        res = trs.execute(input, COST);
        assertEquals(ResultCode.INTERNAL_ERROR, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
    }

    @Test
    public void testRefundAccountNotInContract() {
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address acct2 = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);

        // acct2 has never deposited and is not a valid account in the contract yet.
        byte[] input = getRefundInput(contract, acct2, BigInteger.ONE);
        TRSuseContract trs = newTRSuseContract(acct2);
        ContractExecutionResult res = trs.execute(input, COST);
        assertEquals(ResultCode.INTERNAL_ERROR, res.getResultCode());
        assertEquals(0, res.getNrgLeft());

        // Have others deposit but not acct2 and try again ... should be same result.
        Address acct3 = getNewExistentAccount(DEFAULT_BALANCE);
        Address acct4 = getNewExistentAccount(DEFAULT_BALANCE);

        input = getDepositInput(contract, DEFAULT_BALANCE);
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(acct3).execute(input, COST).getResultCode());
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(acct4).execute(input, COST).getResultCode());

        input = getRefundInput(contract, acct2, BigInteger.ONE);
        res = newTRSuseContract(acct2).execute(input, COST);
        assertEquals(ResultCode.INTERNAL_ERROR, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
    }

    @Test
    public void testRefundContractIsLocked() {
        Address acct = getNewExistentAccount(BigInteger.ONE);
        Address acct2 = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);

        // Have acct2 deposit some balance.
        byte[] input = getDepositInput(contract, DEFAULT_BALANCE);
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(acct2).execute(input, COST).getResultCode());

        // Now lock the contract.
        input = getLockInput(contract);
        assertEquals(ResultCode.SUCCESS, newTRSstateContract(acct).execute(input, COST).getResultCode());

        // Now have contract owner try to refund acct2.
        input = getRefundInput(contract, acct2, BigInteger.ONE);
        ContractExecutionResult res = newTRSuseContract(acct).execute(input, COST);
        assertEquals(ResultCode.INTERNAL_ERROR, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
    }

    @Test
    public void testRefundContractIsLive() {
        Address acct = getNewExistentAccount(BigInteger.ONE);
        Address acct2 = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);

        // Have acct2 deposit some balance.
        byte[] input = getDepositInput(contract, DEFAULT_BALANCE);
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(acct2).execute(input, COST).getResultCode());

        // Now lock the contract and make it live.
        input = getLockInput(contract);
        assertEquals(ResultCode.SUCCESS, newTRSstateContract(acct).execute(input, COST).getResultCode());
        input = getStartInput(contract);
        assertEquals(ResultCode.SUCCESS, newTRSstateContract(acct).execute(input, COST).getResultCode());

        // Now have contract owner try to refund acct2.
        input = getRefundInput(contract, acct2, BigInteger.ONE);
        ContractExecutionResult res = newTRSuseContract(acct).execute(input, COST);
        assertEquals(ResultCode.INTERNAL_ERROR, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
    }

    @Test
    public void testRefundAccountBalanceInsufficient() {
        Address acct = getNewExistentAccount(BigInteger.ONE);
        Address acct2 = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);

        // Have acct2 deposit some balance.
        byte[] input = getDepositInput(contract, DEFAULT_BALANCE);
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(acct2).execute(input, COST).getResultCode());

        // Now have contract owner try to refund acct2 for more than acct2 has deposited.
        input = getRefundInput(contract, acct2, DEFAULT_BALANCE.add(BigInteger.ONE));
        ContractExecutionResult res = newTRSuseContract(acct).execute(input, COST);
        assertEquals(ResultCode.INSUFFICIENT_BALANCE, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
    }

    @Test
    public void testRefundAccountFullBalance() {
        Address acct = getNewExistentAccount(BigInteger.ONE);
        Address acct2 = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);

        assertEquals(DEFAULT_BALANCE, repo.getBalance(acct2));

        // Have acct2 deposit some balance.
        byte[] input = getDepositInput(contract, DEFAULT_BALANCE);
        TRSuseContract trs = newTRSuseContract(acct2);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());

        assertEquals(DEFAULT_BALANCE, getDepositBalance(trs, contract, acct2));
        assertEquals(DEFAULT_BALANCE, getTotalBalance(trs, contract));
        assertEquals(BigInteger.ZERO, repo.getBalance(acct2));
        assertTrue(accountIsValid(trs, contract, acct2));

        // Now have contract owner try to refund acct2 for exactly what acct2 has deposited.
        input = getRefundInput(contract, acct2, DEFAULT_BALANCE);
        ContractExecutionResult res = newTRSuseContract(acct).execute(input, COST);
        assertEquals(ResultCode.SUCCESS, res.getResultCode());
        assertEquals(0, res.getNrgLeft());

        assertEquals(BigInteger.ZERO, getDepositBalance(trs, contract, acct2));
        assertEquals(BigInteger.ZERO, getTotalBalance(trs, contract));
        assertEquals(DEFAULT_BALANCE, repo.getBalance(acct2));
        assertFalse(accountIsValid(trs, contract, acct2));
    }

    @Test
    public void testRefundAccountFullBalance2() {
        // Same as above test but we test here a balance that spans multiple storage rows.
        BigInteger max = getMaxOneTimeDeposit();
        Address acct = getNewExistentAccount(max);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);

        assertEquals(max, repo.getBalance(acct));

        byte[] input = getMaxDepositInput(contract);
        TRSuseContract trs = newTRSuseContract(acct);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());

        assertEquals(max, getDepositBalance(trs, contract, acct));
        assertEquals(max, getTotalBalance(trs, contract));
        assertEquals(BigInteger.ZERO, repo.getBalance(acct));
        assertTrue(accountIsValid(trs, contract, acct));

        input = getMaxRefundInput(contract, acct);
        ContractExecutionResult res = newTRSuseContract(acct).execute(input, COST);
        assertEquals(ResultCode.SUCCESS, res.getResultCode());
        assertEquals(0, res.getNrgLeft());

        assertEquals(BigInteger.ZERO, getDepositBalance(trs, contract, acct));
        assertEquals(BigInteger.ZERO, getTotalBalance(trs, contract));
        assertEquals(max, repo.getBalance(acct));
        assertFalse(accountIsValid(trs, contract, acct));
    }

    @Test
    public void testRefundAccountBalanceLeftover() {
        BigInteger max = getMaxOneTimeDeposit();
        Address acct = getNewExistentAccount(max);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);

        assertEquals(max, repo.getBalance(acct));

        BigInteger depositAmt = new BigInteger("897326236725789012");
        byte[] input = getDepositInput(contract, depositAmt);
        TRSuseContract trs = newTRSuseContract(acct);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());

        assertEquals(depositAmt, getDepositBalance(trs, contract, acct));
        assertEquals(depositAmt, getTotalBalance(trs, contract));
        assertEquals(max.subtract(depositAmt), repo.getBalance(acct));
        assertTrue(accountIsValid(trs, contract, acct));

        BigInteger diff = new BigInteger("23478523");
        input = getRefundInput(contract, acct, depositAmt.subtract(diff));
        ContractExecutionResult res = newTRSuseContract(acct).execute(input, COST);
        assertEquals(ResultCode.SUCCESS, res.getResultCode());
        assertEquals(0, res.getNrgLeft());

        assertEquals(diff, getDepositBalance(trs, contract, acct));
        assertEquals(diff, getTotalBalance(trs, contract));
        assertEquals(max.subtract(diff), repo.getBalance(acct));
        assertTrue(accountIsValid(trs, contract, acct));
    }

    @Test
    public void testRefundTotalBalanceMultipleAccounts() {
        BigInteger funds1 = new BigInteger("439378943235235");
        BigInteger funds2 = new BigInteger("298598364");
        BigInteger funds3 = new BigInteger("9832958020263806345437400898000");
        Address acct1 = getNewExistentAccount(funds1);
        Address acct2 = getNewExistentAccount(funds2);
        Address acct3 = getNewExistentAccount(funds3);
        Address contract = createTRScontract(acct1, false, true, 1,
            BigInteger.ZERO, 0);

        // Make some deposits.
        byte[] input = getDepositInput(contract, funds1);
        TRSuseContract trs = newTRSuseContract(acct1);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());
        input = getDepositInput(contract, funds2);
        trs = newTRSuseContract(acct2);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());
        input = getDepositInput(contract, funds3);
        trs = newTRSuseContract(acct3);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());

        assertEquals(funds1, getDepositBalance(trs, contract, acct1));
        assertEquals(funds2, getDepositBalance(trs, contract, acct2));
        assertEquals(funds3, getDepositBalance(trs, contract, acct3));
        assertEquals(funds1.add(funds2).add(funds3), getTotalBalance(trs, contract));
        assertTrue(accountIsValid(trs, contract, acct1));
        assertTrue(accountIsValid(trs, contract, acct2));
        assertTrue(accountIsValid(trs, contract, acct3));

        // Make some refunds.
        BigInteger diff1 = new BigInteger("34645642");
        BigInteger diff2 = new BigInteger("196254756");
        input = getRefundInput(contract, acct1, funds1.subtract(diff1));
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(acct1).execute(input, COST).getResultCode());
        input = getRefundInput(contract, acct2, funds2.subtract(diff2));
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(acct1).execute(input, COST).getResultCode());
        input = getRefundInput(contract, acct3, funds3);
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(acct1).execute(input, COST).getResultCode());

        assertEquals(diff1, getDepositBalance(trs, contract, acct1));
        assertEquals(diff2, getDepositBalance(trs, contract, acct2));
        assertEquals(BigInteger.ZERO, getDepositBalance(trs, contract, acct3));
        assertEquals(diff1.add(diff2), getTotalBalance(trs, contract));
        assertTrue(accountIsValid(trs, contract, acct1));
        assertTrue(accountIsValid(trs, contract, acct2));
        assertFalse(accountIsValid(trs, contract, acct3));
    }

    @Test
    public void testRefundInvalidAccount() {
        // We make an account invalid by depositing and then fully refunding it.
        Address acct = getNewExistentAccount(BigInteger.ONE);
        Address acct2 = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);

        assertEquals(DEFAULT_BALANCE, repo.getBalance(acct2));

        byte[] input = getDepositInput(contract, DEFAULT_BALANCE);
        TRSuseContract trs = newTRSuseContract(acct2);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());

        assertEquals(DEFAULT_BALANCE, getDepositBalance(trs, contract, acct2));
        assertEquals(DEFAULT_BALANCE, getTotalBalance(trs, contract));
        assertEquals(BigInteger.ZERO, repo.getBalance(acct2));
        assertTrue(accountIsValid(trs, contract, acct2));

        input = getRefundInput(contract, acct2, DEFAULT_BALANCE);
        ContractExecutionResult res = newTRSuseContract(acct).execute(input, COST);
        assertEquals(ResultCode.SUCCESS, res.getResultCode());
        assertEquals(0, res.getNrgLeft());

        assertEquals(BigInteger.ZERO, getDepositBalance(trs, contract, acct2));
        assertEquals(BigInteger.ZERO, getTotalBalance(trs, contract));
        assertEquals(DEFAULT_BALANCE, repo.getBalance(acct2));
        assertFalse(accountIsValid(trs, contract, acct2));

        // Now try to refund acct2 again...
        input = getRefundInput(contract, acct2, BigInteger.ONE);
        res = newTRSuseContract(acct).execute(input, COST);
        assertEquals(ResultCode.INTERNAL_ERROR, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
    }

    @Test
    public void testRefundZeroForNonExistentAccount() {
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address acct2 = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);
        byte[] input = getRefundInput(contract, acct2, BigInteger.ZERO);
        TRSuseContract trs = newTRSuseContract(acct);
        ContractExecutionResult res = trs.execute(input, COST);
        assertEquals(ResultCode.INTERNAL_ERROR, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
    }

    @Test
    public void testRefundZeroForInvalidAccount() {
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address acct2 = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);

        byte[] input = getDepositInput(contract, DEFAULT_BALANCE);
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(acct2).execute(input, COST).getResultCode());
        input = getRefundInput(contract, acct2, DEFAULT_BALANCE);
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(acct).execute(input, COST).getResultCode());

        // Acct2 is now marked invalid.
        input = getRefundInput(contract, acct2, BigInteger.ZERO);
        assertEquals(ResultCode.INTERNAL_ERROR, newTRSuseContract(acct).execute(input, COST).getResultCode());
    }

    @Test
    public void testRefundZeroForValidAccount() {
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address acct2 = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);

        byte[] input = getDepositInput(contract, DEFAULT_BALANCE);
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(acct2).execute(input, COST).getResultCode());

        // Now try to refund nothing, acct2 exists in the contract.
        input = getRefundInput(contract, acct2, BigInteger.ZERO);
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(acct).execute(input, COST).getResultCode());

        // Verify nothing actually changed.
        TRSuseContract trs = newTRSuseContract(acct);
        assertEquals(DEFAULT_BALANCE, getDepositBalance(trs, contract, acct2));
        assertEquals(DEFAULT_BALANCE, getTotalBalance(trs, contract));
        assertEquals(BigInteger.ZERO, repo.getBalance(acct2));
    }

    @Test
    public void testRefundSuccessNrgLeft() {
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address acct2 = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);

        byte[] input = getDepositInput(contract, DEFAULT_BALANCE);
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(acct2).execute(input, COST).getResultCode());

        long diff = 47835;
        input = getRefundInput(contract, acct2, BigInteger.ZERO);
        ContractExecutionResult res = newTRSuseContract(acct).execute(input, COST + diff);
        assertEquals(ResultCode.SUCCESS, res.getResultCode());
        assertEquals(diff, res.getNrgLeft());
    }

    // <---------------------------------TRS DEPOSIT-FOR TESTS------------------------------------->
    // NOTE: the actual deposit logic occurs on 1 code path, which deposit and depositFor share. The
    // deposit tests have more extensive tests for correctness.

    @Test
    public void testDepositForInputTooLong() {
        Address acct = getNewExistentAccount(BigInteger.ONE);
        Address contract = createTRScontract(acct, false, false, 1,
            BigInteger.ZERO, 0);

        byte[] input = getDepositForInput(contract, acct, BigInteger.ONE);
        byte[] longInput = new byte[input.length + 1];
        System.arraycopy(input, 0, longInput, 0, input.length);

        assertEquals(ResultCode.INTERNAL_ERROR, newTRSuseContract(acct).execute(longInput, COST).getResultCode());
    }

    @Test
    public void testDepositForInputTooShort() {
        Address acct = getNewExistentAccount(BigInteger.ONE);
        Address contract = createTRScontract(acct, false, false, 1,
            BigInteger.ZERO, 0);

        byte[] input = getDepositForInput(contract, acct, BigInteger.ONE);
        byte[] shortInput = new byte[input.length - 1];
        System.arraycopy(input, 0, shortInput, 0, input.length - 1);

        assertEquals(ResultCode.INTERNAL_ERROR, newTRSuseContract(acct).execute(shortInput, COST).getResultCode());
    }

    @Test
    public void testDepositForContractIsLocked() {
        Address acct = getNewExistentAccount(BigInteger.ONE);
        Address contract = createAndLockTRScontract(acct, false, false, 1,
            BigInteger.ZERO, 0);

        byte[] input = getDepositForInput(contract, acct, BigInteger.ONE);
        ContractExecutionResult res = newTRSuseContract(acct).execute(input, COST);
        assertEquals(ResultCode.INTERNAL_ERROR, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
    }

    @Test
    public void testDepositForContractIsLive() {
        Address acct = getNewExistentAccount(BigInteger.ONE);
        Address contract = createLockedAndLiveTRScontract(acct, false, false,
            1, BigInteger.ZERO, 0);

        byte[] input = getDepositForInput(contract, acct, BigInteger.ONE);
        ContractExecutionResult res = newTRSuseContract(acct).execute(input, COST);
        assertEquals(ResultCode.INTERNAL_ERROR, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
    }

    @Test
    public void testDepositForCallerIsNotOwner() {
        Address acct = getNewExistentAccount(BigInteger.ONE);
        Address whoami = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, false, 1,
            BigInteger.ZERO, 0);

        byte[] input = getDepositForInput(contract, whoami, DEFAULT_BALANCE);
        ContractExecutionResult res = newTRSuseContract(whoami).execute(input, COST);
        assertEquals(ResultCode.INTERNAL_ERROR, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
    }

    @Test
    public void testDepositForCallerHasInsufficientFunds() {
        // Owner does not have adequate funds but the recipient of the deposit-for does.
        Address owner = getNewExistentAccount(BigInteger.ONE);
        Address other = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(owner, false, false, 1,
            BigInteger.ZERO, 0);

        byte[] input = getDepositForInput(contract, other, DEFAULT_BALANCE);
        ContractExecutionResult res = newTRSuseContract(owner).execute(input, COST);
        assertEquals(ResultCode.INSUFFICIENT_BALANCE, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
    }

    @Test
    public void testDepositForNonAionAddressAsDepositor() {
        // The deposit-for recipient is another trs contract...
        Address owner = getNewExistentAccount(BigInteger.ONE);
        Address other = createTRScontract(owner, false, false, 1,
            BigInteger.ZERO, 0);
        Address contract = createTRScontract(owner, false, false, 1,
            BigInteger.ZERO, 0);

        byte[] input = getDepositForInput(contract, other, BigInteger.ONE);
        ContractExecutionResult res = newTRSuseContract(owner).execute(input, COST);
        assertEquals(ResultCode.INTERNAL_ERROR, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
    }

    @Test
    public void testDepositForContractAddressIsInvalid() {
        Address owner = getNewExistentAccount(BigInteger.ONE);
        Address other = getNewExistentAccount(DEFAULT_BALANCE);
        byte[] addr = Arrays.copyOf(other.toBytes(), other.toBytes().length);
        addr[0] = (byte) 0xC0;
        Address contract = new Address(addr);

        byte[] input = getDepositForInput(contract, other, BigInteger.ONE);
        ContractExecutionResult res = newTRSuseContract(owner).execute(input, COST);
        assertEquals(ResultCode.INTERNAL_ERROR, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
    }

    @Test
    public void testDepositForZeroAmount() {
        Address owner = getNewExistentAccount(BigInteger.ONE);
        Address other = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(owner, false, false, 1,
            BigInteger.ZERO, 0);

        // Verify other has zero balance after this (also owner just to make sure)
        AbstractTRS trs = newTRSuseContract(owner);
        byte[] input = getDepositForInput(contract, other, BigInteger.ZERO);
        ContractExecutionResult res = trs.execute(input, COST);
        assertEquals(ResultCode.SUCCESS, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
        assertEquals(BigInteger.ZERO, getDepositBalance(trs, contract, other));
        assertEquals(BigInteger.ZERO, getDepositBalance(trs, contract, owner));
        assertEquals(BigInteger.ONE, repo.getBalance(owner));
    }

    @Test
    public void testDepositForOneAccount() {
        // other does not need any funds.
        Address owner = getNewExistentAccount(DEFAULT_BALANCE);
        Address other = getNewExistentAccount(BigInteger.ZERO);
        Address contract = createTRScontract(owner, false, false, 1,
            BigInteger.ZERO, 0);

        // Verify other has zero balance after this (also owner just to make sure)
        AbstractTRS trs = newTRSuseContract(owner);
        byte[] input = getDepositForInput(contract, other, DEFAULT_BALANCE);
        ContractExecutionResult res = trs.execute(input, COST);
        assertEquals(ResultCode.SUCCESS, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
        assertEquals(DEFAULT_BALANCE, getDepositBalance(trs, contract, other));
        assertEquals(BigInteger.ZERO, getDepositBalance(trs, contract, owner));
        assertEquals(BigInteger.ZERO, repo.getBalance(owner));
    }

    @Test
    public void testDepositForMultipleAccounts() {
        BigInteger balance = new BigInteger("832523626");
        Address owner = getNewExistentAccount(balance.multiply(BigInteger.valueOf(4)));
        Address other1 = getNewExistentAccount(BigInteger.ZERO);
        Address other2 = getNewExistentAccount(BigInteger.ZERO);
        Address other3 = getNewExistentAccount(BigInteger.ZERO);
        Address contract = createTRScontract(owner, false, false, 1,
            BigInteger.ZERO, 0);

        AbstractTRS trs = newTRSuseContract(owner);
        byte[] input = getDepositForInput(contract, other1, balance);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());
        input = getDepositForInput(contract, other2, balance);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());
        input = getDepositForInput(contract, other3, balance);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());

        assertEquals(balance, getDepositBalance(trs, contract, other1));
        assertEquals(balance, getDepositBalance(trs, contract, other2));
        assertEquals(balance, getDepositBalance(trs, contract, other3));
        assertEquals(BigInteger.ZERO, getDepositBalance(trs, contract, owner));
        assertEquals(balance, repo.getBalance(owner));
    }

    @Test
    public void testDepositForSameAccountMultipleTimes() {
        BigInteger balance = new BigInteger("8293652893346342674375477457554345");
        int times = 61;
        Address owner = getNewExistentAccount((balance.multiply(BigInteger.TWO)).
            multiply(BigInteger.valueOf(times)));
        Address other1 = getNewExistentAccount(BigInteger.ZERO);
        Address other2 = getNewExistentAccount(BigInteger.ZERO);
        Address contract = createTRScontract(owner, false, false, 1,
            BigInteger.ZERO, 0);

        AbstractTRS trs = newTRSuseContract(owner);
        byte[] input = getDepositForInput(contract, other1, balance);
        for (int i = 0; i < times; i++) {
            assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());
        }
        input = getDepositForInput(contract, other2, balance);
        for (int i = 0; i < times; i++) {
            assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());
        }

        assertEquals(balance.multiply(BigInteger.valueOf(times)), getDepositBalance(trs, contract, other1));
        assertEquals(balance.multiply(BigInteger.valueOf(times)), getDepositBalance(trs, contract, other2));
        assertEquals(BigInteger.ZERO, getDepositBalance(trs, contract, owner));
        assertEquals(BigInteger.ZERO, repo.getBalance(owner));
    }

    @Test
    public void testDepositForOneself() {
        // No reason why an owner can't use depositFor to deposit on his/her own behalf.
        Address owner = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(owner, false, false, 1,
            BigInteger.ZERO, 0);

        // Verify other has zero balance after this (also owner just to make sure)
        AbstractTRS trs = newTRSuseContract(owner);
        byte[] input = getDepositForInput(contract, owner, DEFAULT_BALANCE);
        ContractExecutionResult res = trs.execute(input, COST);
        assertEquals(ResultCode.SUCCESS, res.getResultCode());
        assertEquals(0, res.getNrgLeft());
        assertEquals(DEFAULT_BALANCE, getDepositBalance(trs, contract, owner));
        assertEquals(BigInteger.ZERO, repo.getBalance(owner));
    }

    // <-------------------------------TRS ADD EXTRA FUNDS TESTS----------------------------------->

    @Test
    public void testAddExtraInputTooShort() {
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);
        byte[] input = getAddExtraInput(contract, DEFAULT_BALANCE);
        byte[] shortInput = Arrays.copyOf(input, input.length - 1);
        assertEquals(ResultCode.INTERNAL_ERROR, newTRSuseContract(acct).execute(shortInput, COST).getResultCode());
    }

    @Test
    public void testAddExtraInputTooLong() {
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);
        byte[] input = getAddExtraInput(contract, DEFAULT_BALANCE);
        byte[] longInput = new byte[input.length + 1];
        System.arraycopy(input, 0, longInput, 0, input.length);
        assertEquals(ResultCode.INTERNAL_ERROR, newTRSuseContract(acct).execute(longInput, COST).getResultCode());
    }

    @Test
    public void testAddExtraContractNonExistent() {
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        byte[] input = getAddExtraInput(acct, DEFAULT_BALANCE);
        assertEquals(ResultCode.INTERNAL_ERROR, newTRSuseContract(acct).execute(input, COST).getResultCode());
    }

    @Test
    public void testAddExtraCallerIsNotOwner() {
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(AION, false, true, 1,
            BigInteger.ZERO, 0);
        byte[] input = getAddExtraInput(contract, DEFAULT_BALANCE);
        assertEquals(ResultCode.INTERNAL_ERROR, newTRSuseContract(acct).execute(input, COST).getResultCode());
    }

    @Test
    public void testAddExtraCallHasInsufficientFunds() {
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);
        byte[] input = getAddExtraInput(contract, DEFAULT_BALANCE.add(BigInteger.ONE));
        assertEquals(ResultCode.INSUFFICIENT_BALANCE, newTRSuseContract(acct).execute(input, COST).getResultCode());
    }

    @Test
    public void testAddExtraFundsOpen() {
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);
        byte[] input = getOpenFundsInput(contract);
        assertEquals(ResultCode.SUCCESS, newTRSstateContract(acct).execute(input, COST).getResultCode());
        assertTrue(getAreContractFundsOpen(newTRSstateContract(acct), contract));

        input = getAddExtraInput(contract, DEFAULT_BALANCE);
        assertEquals(ResultCode.INTERNAL_ERROR, newTRSuseContract(acct).execute(input, COST).getResultCode());
    }

    @Test
    public void testExtraFundsNewContract() {
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);

        AbstractTRS trs = newTRSuseContract(acct);
        assertEquals(BigInteger.ZERO, grabExtraFunds(trs, contract));
    }

    @Test
    public void testAddZeroExtraFunds() {
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);

        AbstractTRS trs = newTRSuseContract(acct);
        byte[] input = getAddExtraInput(contract, BigInteger.ZERO);
        assertEquals(ResultCode.INTERNAL_ERROR, trs.execute(input, COST).getResultCode());
        assertEquals(BigInteger.ZERO, grabExtraFunds(trs, contract));
        assertEquals(DEFAULT_BALANCE, repo.getBalance(acct));
    }

    @Test
    public void testAddExtraFundsUnlocked() {
        BigInteger amt = new BigInteger("32985623956237896532753265332");
        Address acct = getNewExistentAccount(amt);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);

        AbstractTRS trs = newTRSuseContract(acct);
        byte[] input = getAddExtraInput(contract, amt);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());
        assertEquals(amt, grabExtraFunds(trs, contract));
        assertEquals(BigInteger.ZERO, repo.getBalance(acct));
    }

    @Test
    public void testAddExtraFundsLocked() {
        BigInteger amt = new BigInteger("32985623956237896532753265332").add(BigInteger.ONE);
        Address acct = getNewExistentAccount(amt);
        Address contract = createAndLockTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);

        AbstractTRS trs = newTRSuseContract(acct);
        byte[] input = getAddExtraInput(contract, amt.subtract(BigInteger.ONE));
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());
        assertEquals(amt.subtract(BigInteger.ONE), grabExtraFunds(trs, contract));
        assertEquals(BigInteger.ZERO, repo.getBalance(acct));
    }

    @Test
    public void testAddExtraFundsLive() {
        BigInteger amt = new BigInteger("32985623956237896532753265332").add(BigInteger.ONE);
        Address acct = getNewExistentAccount(amt);
        Address contract = createLockedAndLiveTRScontract(acct, false, true,
            1, BigInteger.ZERO, 0);

        AbstractTRS trs = newTRSuseContract(acct);
        byte[] input = getAddExtraInput(contract, amt.subtract(BigInteger.ONE));
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());
        assertEquals(amt.subtract(BigInteger.ONE), grabExtraFunds(trs, contract));
        assertEquals(BigInteger.ZERO, repo.getBalance(acct));
    }

    @Test
    public void testAddMaxExtraFunds() {
        int times = 23;
        BigInteger amt = getMaxOneTimeDeposit().multiply(BigInteger.valueOf(times));
        Address acct = getNewExistentAccount(amt);
        Address contract = createTRScontract(acct, false, true,
            1, BigInteger.ZERO, 0);

        AbstractTRS trs = newTRSuseContract(acct);
        byte[] input = getAddExtraMaxInput(contract);
        for (int i = 0; i < times; i++) {
            assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());
        }
        assertEquals(BigInteger.ZERO, repo.getBalance(acct));
        assertEquals(amt, grabExtraFunds(trs, contract));
    }

    @Test
    public void testAddExtraDuringSpecialWithdrawPeriod() throws InterruptedException {
        int numDepositors = 7;
        int periods = 4;
        BigInteger deposits = new BigInteger("2389562389532434346345634");
        BigInteger bonus = new BigInteger("237856238756235");
        BigInteger extra = new BigInteger("32865237523");
        BigDecimal percent = new BigDecimal("41.12221");
        Address contract = setupContract(numDepositors, deposits, bonus, periods, percent);

        // In the special-only period, extra funds are unable to be withdrawn.
        AbstractTRS trs = newTRSuseContract(AION);
        repo.addBalance(AION, extra);
        byte[] input = getAddExtraInput(contract, extra);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());
        createBlockchain(0, 0);

        Set<Address> contributors = getAllDepositors(trs, contract);
        BigInteger total = deposits.multiply(BigInteger.valueOf(numDepositors));
        for (Address acc : contributors) {
            BigInteger amt = expectedAmtFirstWithdraw(trs, contract, deposits, total, bonus, percent, periods);
            input = getWithdrawInput(contract);
            assertEquals(ResultCode.SUCCESS, newTRSuseContract(acc).execute(input, COST).getResultCode());
            assertEquals(amt, repo.getBalance(acc));
        }
    }

    @Test
    public void testAddExtraNonFinalPeriod() throws InterruptedException {
        int numDepositors = 4;
        int periods = 8;
        BigInteger deposits = new BigInteger("238752378652");
        BigInteger bonus = new BigInteger("23454234");
        BigInteger extra = new BigInteger("43895634825643872563478934");
        BigDecimal percent = new BigDecimal("1");
        Address contract = setupContract(numDepositors, deposits, bonus, periods, percent);

        AbstractTRS trs = newTRSuseContract(AION);
        repo.addBalance(AION, extra);
        byte[] input = getAddExtraInput(contract, extra);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());
        createBlockchain(1, TimeUnit.SECONDS.toMillis(2));
        int currPeriod = getContractCurrentPeriod(trs, contract);
        assertTrue(currPeriod < periods);
        checkPayoutsNonFinal(trs, contract, numDepositors, deposits, bonus, percent, periods, currPeriod);
    }

    @Test
    public void testAddExtraFinalPeriod() throws InterruptedException {
        int numDepositors = 14;
        int periods = 2;
        BigInteger deposits = new BigInteger("2358325346");
        BigInteger bonus = new BigInteger("5454534");
        BigInteger extra = new BigInteger("34238462353567234");
        BigDecimal percent = new BigDecimal("16");
        Address contract = setupContract(numDepositors, deposits, bonus, periods, percent);

        AbstractTRS trs = newTRSuseContract(AION);
        createBlockchain(1, TimeUnit.SECONDS.toMillis(3));
        int currPeriod = getContractCurrentPeriod(trs, contract);
        assertEquals(currPeriod, periods);

        repo.addBalance(AION, extra);
        byte[] input = getAddExtraInput(contract, extra);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());
        checkPayoutsFinal(trs, contract, numDepositors, deposits, bonus, extra);
    }

    @Test
    public void testAddExtraWithdrawMultipleTimesSpecialPeriod() throws InterruptedException {
        int numDepositors = 7;
        int periods = 4;
        BigInteger deposits = new BigInteger("243346234453");
        BigInteger bonus = new BigInteger("436436343434");
        BigInteger extra = new BigInteger("457457457454856986786534");
        BigDecimal percent = new BigDecimal("11.123");
        Address contract = setupContract(numDepositors, deposits, bonus, periods, percent);

        AbstractTRS trs = newTRSuseContract(AION);
        repo.addBalance(AION, extra);
        byte[] input = getAddExtraInput(contract, extra);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());
        createBlockchain(0, 0);

        // We have half the contributors withdraw now. No extras are collected.
        Set<Address> contributors = getAllDepositors(trs, contract);
        BigInteger total = deposits.multiply(BigInteger.valueOf(numDepositors));

        input = getWithdrawInput(contract);
        boolean withdraw = true;
        for (Address acc : contributors) {
            BigInteger amt = expectedAmtFirstWithdraw(trs, contract, deposits, total, bonus, percent, periods);
            if (withdraw) {
                assertEquals(ResultCode.SUCCESS, newTRSuseContract(acc).execute(input, COST).getResultCode());
                assertEquals(amt, repo.getBalance(acc));
            } else {
                assertEquals(BigInteger.ZERO, repo.getBalance(acc));
            }
            withdraw = !withdraw;
        }

        // Add more extra funds into contract, everyone withdraws. The previous withdrawers should
        // be unable to withdraw and have same balance as before but new ones should get the new
        // extra shares.
        repo.addBalance(AION, extra);
        input = getAddExtraInput(contract, extra);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());

        input = getWithdrawInput(contract);
        withdraw = true;
        for (Address acc : contributors) {
            BigInteger amt = expectedAmtFirstWithdraw(trs, contract, deposits, total, bonus, percent, periods);
            if (withdraw) {
                // These accounts have already withdrawn this period.
                assertEquals(ResultCode.INTERNAL_ERROR, newTRSuseContract(acc).execute(input, COST).getResultCode());
            } else {
                assertEquals(ResultCode.SUCCESS, newTRSuseContract(acc).execute(input, COST).getResultCode());
            }
            assertEquals(amt, repo.getBalance(acc));
            withdraw = !withdraw;
        }
    }

    @Test
    public void testAddExtraWithdrawMultipleTimesSameNonFinalPeriod() throws InterruptedException {
        int numDepositors = 17;
        int periods = 8;
        BigInteger deposits = new BigInteger("43436454575475437");
        BigInteger bonus = new BigInteger("325436346546345634634634346");
        BigInteger extra = new BigInteger("543435325634674563434");
        BigDecimal percent = new BigDecimal("18.888");
        Address contract = setupContract(numDepositors, deposits, bonus, periods, percent);

        AbstractTRS trs = newTRSuseContract(AION);
        repo.addBalance(AION, extra);
        byte[] input = getAddExtraInput(contract, extra);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());
        createBlockchain(1, TimeUnit.SECONDS.toMillis(2));
        int currPeriod = getContractCurrentPeriod(trs, contract);
        assertTrue(currPeriod < periods);

        // We add some extra funds and half the accounts make a withdrawal, half don't.
        repo.addBalance(AION, extra);
        input = getAddExtraInput(contract, extra);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());

        Set<Address> contributors = getAllDepositors(trs, contract);
        BigInteger total = deposits.multiply(BigInteger.valueOf(numDepositors));
        BigDecimal fraction = BigDecimal.ONE.divide(BigDecimal.valueOf(numDepositors), 18, RoundingMode.HALF_DOWN);

        BigInteger prevAmt = null;
        input = getWithdrawInput(contract);
        boolean withdraw = true;
        for (Address acc : contributors) {
            BigInteger extraShare = getExtraShare(trs, contract, acc, fraction, currPeriod);
            BigInteger amt = expectedAmtFirstWithdraw(trs, contract, deposits, total, bonus, percent, periods);
            prevAmt = amt.add(extraShare);
            if (withdraw) {
                assertEquals(ResultCode.SUCCESS, newTRSuseContract(acc).execute(input, COST).getResultCode());
                assertEquals(amt.add(extraShare), repo.getBalance(acc));
            } else {
                assertEquals(BigInteger.ZERO, repo.getBalance(acc));
            }
            withdraw = !withdraw;
        }

        // Now we add more extra funds and have everyone withdraw. The ones who have withdrawn
        // already will fail and the new ones will collect more.
        repo.addBalance(AION, extra);
        input = getAddExtraInput(contract, extra);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());

        input = getWithdrawInput(contract);
        withdraw = true;
        for (Address acc : contributors) {
            BigInteger extraShare = getExtraShare(trs, contract, acc, fraction, currPeriod);
            BigInteger amt = expectedAmtFirstWithdraw(trs, contract, deposits, total, bonus, percent, periods);
            if (withdraw) {
                // These have already withdrawn.
                assertEquals(ResultCode.INTERNAL_ERROR, newTRSuseContract(acc).execute(input, COST).getResultCode());
                assertEquals(prevAmt, repo.getBalance(acc));
            } else {
                assertEquals(ResultCode.SUCCESS, newTRSuseContract(acc).execute(input, COST).getResultCode());
                assertEquals(amt.add(extraShare), repo.getBalance(acc));
                assertTrue(prevAmt.compareTo(amt.add(extraShare)) < 0);
            }
            withdraw = !withdraw;
        }
    }

    @Test
    public void testAddExtraWithdrawMultipleTimesFinalPeriod() {
        //TODO -- likely need small tweak; atm 1 withdraw per period, need an exception for final period.
    }

    @Test
    public void testAddExtraMultipleTimesMultipleWithdrawsOverContractLifetime() {
        //TODO
    }

    // <----------------------------TRS DEPOSITOR LINKED LIST TESTS-------------------------------->

    @Test
    public void testLinkedListNoDepositors() {
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);
        TRSuseContract trs = newTRSuseContract(acct);
        assertNull(getLinkedListHead(trs, contract));
    }

    @Test
    public void testLinkedListOneDepositor() {
        // First test using deposit.
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);
        byte[] input = getDepositInput(contract, BigInteger.ONE);
        TRSuseContract trs = newTRSuseContract(acct);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());
        checkLinkedListOneDepositor(trs, contract, acct, input);

        repo.incrementNonce(acct);

        // Now test using depositFor.
        contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);
        input = getDepositForInput(contract, acct, BigInteger.ONE);
        trs = newTRSuseContract(acct);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());
        checkLinkedListOneDepositor(trs, contract, acct, input);
    }

    @Test
    public void testLinkedListTwoDepositors() {
        // First test using deposit.
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address acct2 = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);
        byte[] input = getDepositInput(contract, BigInteger.ONE);

        TRSuseContract trs = newTRSuseContract(acct);
        trs.execute(input, COST);

        trs = newTRSuseContract(acct2);
        trs.execute(input, COST);
        trs.execute(input, COST);

        trs = newTRSuseContract(acct);
        trs.execute(input, COST);

        checkLinkedListTwoDepositors(trs, contract, acct, acct2);

        // Test using depositFor.
        repo.incrementNonce(acct);
        repo.incrementNonce(acct2);
        contract = createTRScontract(acct, false, false, 1,
            BigInteger.ZERO, 0);
        input = getDepositForInput(contract, acct, BigInteger.ONE);

        trs = newTRSuseContract(acct);
        trs.execute(input, COST);

        input = getDepositForInput(contract, acct2, BigInteger.ONE);
        trs = newTRSuseContract(acct);
        trs.execute(input, COST);
        trs.execute(input, COST);

        input = getDepositForInput(contract, acct, BigInteger.ONE);
        trs = newTRSuseContract(acct);
        trs.execute(input, COST);

        checkLinkedListTwoDepositors(trs, contract, acct, acct2);
    }

    @Test
    public void testLinkedListMultipleDepositors() {
        // First test using deposit.
        Address acct1, acct2, acct3, acct4;
        acct1 = getNewExistentAccount(DEFAULT_BALANCE);
        acct2 = getNewExistentAccount(DEFAULT_BALANCE);
        acct3 = getNewExistentAccount(DEFAULT_BALANCE);
        acct4 = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct1, false, true, 1,
            BigInteger.ZERO, 0);
        byte[] input = getDepositInput(contract, BigInteger.ONE);

        newTRSuseContract(acct1).execute(input, COST);
        newTRSuseContract(acct4).execute(input, COST);
        newTRSuseContract(acct2).execute(input, COST);
        newTRSuseContract(acct4).execute(input, COST);
        newTRSuseContract(acct1).execute(input, COST);
        newTRSuseContract(acct3).execute(input, COST);
        newTRSuseContract(acct1).execute(input, COST);

        checkLinkedListMultipleDepositors(contract, acct1, acct2, acct3, acct4);

        // Test using depositFor.
        acct1 = getNewExistentAccount(DEFAULT_BALANCE);
        acct2 = getNewExistentAccount(DEFAULT_BALANCE);
        acct3 = getNewExistentAccount(DEFAULT_BALANCE);
        acct4 = getNewExistentAccount(DEFAULT_BALANCE);
        contract = createTRScontract(acct1, false, false, 1,
            BigInteger.ZERO, 0);

        input = getDepositForInput(contract, acct1, BigInteger.ONE);
        newTRSuseContract(acct1).execute(input, COST);
        input = getDepositForInput(contract, acct4, BigInteger.ONE);
        newTRSuseContract(acct1).execute(input, COST);
        input = getDepositForInput(contract, acct2, BigInteger.ONE);
        newTRSuseContract(acct1).execute(input, COST);
        input = getDepositForInput(contract, acct4, BigInteger.ONE);
        newTRSuseContract(acct1).execute(input, COST);
        input = getDepositForInput(contract, acct1, BigInteger.ONE);
        newTRSuseContract(acct1).execute(input, COST);
        input = getDepositForInput(contract, acct3, BigInteger.ONE);
        newTRSuseContract(acct1).execute(input, COST);
        input = getDepositForInput(contract, acct1, BigInteger.ONE);
        newTRSuseContract(acct1).execute(input, COST);

        checkLinkedListMultipleDepositors(contract, acct1, acct2, acct3, acct4);
    }

    @Test
    public void testRemoveHeadOfListWithHeadOnly() {
        // Test using deposit.
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);
        byte[] input = getDepositInput(contract, DEFAULT_BALANCE);
        TRSuseContract trs = newTRSuseContract(acct);

        checkRemoveHeadOfListWithHeadOnly(trs, contract, acct, input);

        // Test using depositFor
        acct = getNewExistentAccount(DEFAULT_BALANCE);
        contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);
        input = getDepositForInput(contract, acct, DEFAULT_BALANCE);
        trs = newTRSuseContract(acct);

        checkRemoveHeadOfListWithHeadOnly(trs, contract, acct, input);
    }

    @Test
    public void testRemoveHeadOfListWithHeadAndNextOnly() {
        // Test using deposit.
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address acct2 = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);

        byte[] input = getDepositInput(contract, DEFAULT_BALANCE);
        TRSuseContract trs = newTRSuseContract(acct);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(acct2).execute(input, COST).getResultCode());

        checkRemoveHeadOfListWithHeadAndNextOnly(trs, contract, acct, acct2);

        // Test using depositFor.
        acct = getNewExistentAccount(DEFAULT_BALANCE.multiply(BigInteger.valueOf(2)));
        acct2 = getNewExistentAccount(BigInteger.ZERO);
        contract = createTRScontract(acct, false, false, 1,
            BigInteger.ZERO, 0);

        trs = newTRSuseContract(acct);
        input = getDepositForInput(contract, acct, DEFAULT_BALANCE);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());
        input = getDepositForInput(contract, acct2, DEFAULT_BALANCE);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());

        checkRemoveHeadOfListWithHeadAndNextOnly(trs, contract, acct, acct2);
    }

    @Test
    public void testRemoveHeadOfLargerList() {
        // Test using deposit.
        int listSize = 10;
        Address owner = getNewExistentAccount(BigInteger.ONE);
        Address contract = getContractMultipleDepositors(listSize, owner, false,
            true, 1, BigInteger.ZERO, 0);

        checkRemoveHeadOfLargerList(contract, owner, listSize);

        // Test using depositFor.
        owner = getNewExistentAccount(DEFAULT_BALANCE.multiply(BigInteger.valueOf(listSize)));
        contract = getContractMultipleDepositorsUsingDepositFor(listSize, owner, false,
            1, BigInteger.ZERO, 0);

        checkRemoveHeadOfLargerList(contract, owner, listSize);
    }

    @Test
    public void testRemoveTailOfSizeTwoList() {
        // Test using deposit.
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address acct2 = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);

        byte[] input = getDepositInput(contract, DEFAULT_BALANCE);
        TRSuseContract trs = newTRSuseContract(acct);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(acct2).execute(input, COST).getResultCode());

        checkRemoveTailOfSizeTwoList(trs, contract, acct, acct2);

        // Test using depositFor.
        acct = getNewExistentAccount(DEFAULT_BALANCE.multiply(BigInteger.TWO));
        acct2 = getNewExistentAccount(BigInteger.ZERO);
        contract = createTRScontract(acct, false, false, 1,
            BigInteger.ZERO, 0);

        trs = newTRSuseContract(acct);
        input = getDepositForInput(contract, acct, DEFAULT_BALANCE);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());
        input = getDepositForInput(contract, acct2, DEFAULT_BALANCE);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());

        checkRemoveTailOfSizeTwoList(trs, contract, acct, acct2);
    }

    @Test
    public void testRemoveTailOfLargerList() {
        // Test using deposit.
        int listSize = 10;
        Address owner = getNewExistentAccount(BigInteger.ONE);
        Address contract = getContractMultipleDepositors(listSize, owner, false,
            true, 1, BigInteger.ZERO, 0);

        checkRemoveTailOfLargerList(contract, owner, listSize);

        // Test using depositFor.
        owner = getNewExistentAccount(DEFAULT_BALANCE.multiply(BigInteger.valueOf(listSize)));
        contract = getContractMultipleDepositorsUsingDepositFor(listSize, owner, false,
            1, BigInteger.ZERO, 0);

        checkRemoveTailOfLargerList(contract, owner, listSize);
    }

    @Test
    public void testRemoveInteriorOfSizeThreeList() {
        // Test using deposit.
        Address acct = getNewExistentAccount(DEFAULT_BALANCE);
        Address acct2 = getNewExistentAccount(DEFAULT_BALANCE);
        Address acct3 = getNewExistentAccount(DEFAULT_BALANCE);
        Address contract = createTRScontract(acct, false, true, 1,
            BigInteger.ZERO, 0);

        byte[] input = getDepositInput(contract, DEFAULT_BALANCE);
        TRSuseContract trs = newTRSuseContract(acct);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(acct2).execute(input, COST).getResultCode());
        assertEquals(ResultCode.SUCCESS, newTRSuseContract(acct3).execute(input, COST).getResultCode());

        checkRemoveInteriorOfSizeThreeList(trs, contract, acct, acct2, acct3);

        // Test using depositFor.
        acct = getNewExistentAccount(DEFAULT_BALANCE.multiply(BigInteger.valueOf(3)));
        acct2 = getNewExistentAccount(BigInteger.ZERO);
        acct3 = getNewExistentAccount(BigInteger.ZERO);
        contract = createTRScontract(acct, false, false, 1,
            BigInteger.ZERO, 0);

        trs = newTRSuseContract(acct);
        input = getDepositForInput(contract, acct, DEFAULT_BALANCE);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());
        input = getDepositForInput(contract, acct2, DEFAULT_BALANCE);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());
        input = getDepositForInput(contract, acct3, DEFAULT_BALANCE);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());

        checkRemoveInteriorOfSizeThreeList(trs, contract, acct, acct2, acct3);
    }

    @Test
    public void testRemoveInteriorOfLargerList() {
        // Test using deposit.
        int listSize = 10;
        Address owner = getNewExistentAccount(BigInteger.ONE);
        Address contract = getContractMultipleDepositors(listSize, owner, false,
            true, 1, BigInteger.ZERO, 0);

        checkRemoveInteriorOfLargerList(contract, owner, listSize);

        // Test using depositFor.
        owner = getNewExistentAccount(DEFAULT_BALANCE.multiply(BigInteger.valueOf(listSize)));
        contract = getContractMultipleDepositorsUsingDepositFor(listSize, owner, false,
            1, BigInteger.ZERO, 0);

        checkRemoveInteriorOfLargerList(contract, owner, listSize);
    }

    @Test
    public void testMultipleListRemovals() {
        // Test using deposit.
        int listSize = 10;
        Address owner = getNewExistentAccount(BigInteger.ONE);
        Address contract = getContractMultipleDepositors(listSize, owner, false,
            true, 1, BigInteger.ZERO, 0);

        checkMultipleListRemovals(contract, owner, listSize);

        // Test using depositFor.
        owner = getNewExistentAccount(DEFAULT_BALANCE.multiply(BigInteger.valueOf(listSize)));
        contract = getContractMultipleDepositorsUsingDepositFor(listSize, owner, false,
            1, BigInteger.ZERO, 0);

        checkMultipleListRemovals(contract, owner, listSize);
    }

    private void checkLinkedListOneDepositor(AbstractTRS trs, Address contract, Address acct, byte[] input) {
        assertEquals(acct, getLinkedListHead(trs, contract));
        assertNull(getLinkedListNext(trs, contract, acct));
        assertNull(getLinkedListPrev(trs, contract, acct));

        // Test one depositor makes more than one deposit.
        trs.execute(input, COST);
        trs.execute(input, COST);
        assertEquals(acct, getLinkedListHead(trs, contract));
        assertNull(getLinkedListNext(trs, contract, acct));
        assertNull(getLinkedListPrev(trs, contract, acct));
    }

    // We expect a list with acct2 as head as such: null <- acct2 <-> acct -> null
    private void checkLinkedListTwoDepositors(AbstractTRS trs, Address contract, Address acct,
        Address acct2) {

        assertEquals(acct2, getLinkedListHead(trs, contract));
        assertEquals(acct, getLinkedListNext(trs, contract, acct2));
        assertNull(getLinkedListPrev(trs, contract, acct2));
        assertEquals(acct2, getLinkedListPrev(trs, contract, acct));
        assertNull(getLinkedListNext(trs, contract, acct));
    }

    // Expect a list with acct3 as head as such: null <- acct3 <-> acct2 <-> acct4 <-> acct1 -> null
    private void checkLinkedListMultipleDepositors(Address contract, Address acct1, Address acct2,
        Address acct3, Address acct4) {

        TRSuseContract trs = newTRSuseContract(acct1);
        assertEquals(acct3, getLinkedListHead(trs, contract));
        assertNull(getLinkedListPrev(trs, contract, acct3));
        assertEquals(acct2, getLinkedListNext(trs, contract, acct3));
        assertEquals(acct3, getLinkedListPrev(trs, contract, acct2));
        assertEquals(acct4, getLinkedListNext(trs, contract, acct2));
        assertEquals(acct2, getLinkedListPrev(trs, contract, acct4));
        assertEquals(acct1, getLinkedListNext(trs, contract, acct4));
        assertEquals(acct4, getLinkedListPrev(trs, contract, acct1));
        assertNull(getLinkedListNext(trs, contract, acct1));
    }

    private void checkRemoveHeadOfListWithHeadOnly(AbstractTRS trs, Address contract, Address acct,
        byte[] input) {

        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());

        assertEquals(acct, getLinkedListHead(trs, contract));
        assertNull(getLinkedListNext(trs, contract, acct));
        assertNull(getLinkedListPrev(trs, contract, acct));

        input = getRefundInput(contract, acct, DEFAULT_BALANCE);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());

        assertFalse(accountIsValid(trs, contract, acct));
        assertNull(getLinkedListHead(trs, contract));
    }

    // Expects acct2 as head with:  null <- acct2 <-> acct -> null
    private void checkRemoveHeadOfListWithHeadAndNextOnly(AbstractTRS trs, Address contract,
        Address acct, Address acct2) {

        assertEquals(acct2, getLinkedListHead(trs, contract));
        assertEquals(acct, getLinkedListNext(trs, contract, acct2));
        assertNull(getLinkedListNext(trs, contract, acct));
        assertEquals(acct2, getLinkedListPrev(trs, contract, acct));
        assertNull(getLinkedListPrev(trs, contract, acct2));

        // We remove acct2, the head. Should have:      null <- acct -> null
        byte[] input = getRefundInput(contract, acct2, DEFAULT_BALANCE);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());

        assertFalse(accountIsValid(trs, contract, acct2));
        assertEquals(acct, getLinkedListHead(trs, contract));
        assertNull(getLinkedListNext(trs, contract, acct));
        assertNull(getLinkedListPrev(trs, contract, acct));
    }

    private void checkRemoveHeadOfLargerList(Address contract, Address owner, int listSize) {
        // We have a linked list with 10 depositors. Remove the head.
        TRSuseContract trs = newTRSuseContract(owner);
        Address head = getLinkedListHead(trs, contract);
        Address next = getLinkedListNext(trs, contract, head);
        assertNull(getLinkedListPrev(trs, contract, head));
        assertEquals(head, getLinkedListPrev(trs, contract, next));
        byte[] input = getRefundInput(contract, head, DEFAULT_BALANCE);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());

        // We verify next is the new head, its prev is null and that we can advance 8 times before
        // hitting the end of the list.
        assertEquals(next, getLinkedListHead(trs, contract));
        assertNull(getLinkedListPrev(trs, contract, next));

        // We also make sure each address in the list is unique.
        Set<Address> addressesInList = new HashSet<>();
        for (int i = 0; i < listSize - 1; i++) {
            if (i == listSize - 2) {
                assertNull(getLinkedListNext(trs, contract, next));
            } else {
                next = getLinkedListNext(trs, contract, next);
                assertNotNull(next);
                assertFalse(addressesInList.contains(next));
                addressesInList.add(next);
            }
        }
    }

    // Expects acct2 as head with:  null <- acct2 <-> acct -> null
    private void checkRemoveTailOfSizeTwoList(AbstractTRS trs, Address contract, Address acct,
        Address acct2) {

        assertEquals(acct2, getLinkedListHead(trs, contract));
        assertEquals(acct, getLinkedListNext(trs, contract, acct2));
        assertNull(getLinkedListNext(trs, contract, acct));
        assertEquals(acct2, getLinkedListPrev(trs, contract, acct));
        assertNull(getLinkedListPrev(trs, contract, acct2));

        // We remove acct, the tail. Should have:      null <- acct2 -> null
        byte[] input = getRefundInput(contract, acct, DEFAULT_BALANCE);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());

        assertFalse(accountIsValid(trs, contract, acct));
        assertEquals(acct2, getLinkedListHead(trs, contract));
        assertNull(getLinkedListNext(trs, contract, acct2));
        assertNull(getLinkedListPrev(trs, contract, acct2));
    }

    private void checkRemoveTailOfLargerList(Address contract, Address owner, int listSize) {
        // We have a linked list with 10 depositors. First find the tail. Ensure each address is unique too.
        TRSuseContract trs = newTRSuseContract(owner);
        Address next = getLinkedListHead(trs, contract);
        Address head = new Address(next.toBytes());
        Set<Address> addressesInList = new HashSet<>();
        for (int i = 0; i < listSize; i++) {
            if (i == listSize - 1) {
                assertNull(getLinkedListNext(trs, contract, next));
            } else {
                next = getLinkedListNext(trs, contract, next);
                assertNotNull(next);
                assertFalse(addressesInList.contains(next));
                addressesInList.add(next);
            }
        }

        // Now next should be the tail. Remove it. Iterate over list again.
        byte[] input = getRefundInput(contract, next, DEFAULT_BALANCE);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());
        assertFalse(accountIsValid(trs, contract, next));

        assertEquals(head, getLinkedListHead(trs, contract));
        for (int i = 0; i < listSize - 1; i++) {
            if (i == listSize - 2) {
                assertNull(getLinkedListNext(trs, contract, head));
            } else {
                head = getLinkedListNext(trs, contract, head);
                assertNotNull(head);
                assertTrue(addressesInList.contains(head));
                assertNotEquals(next, head);
            }
        }
    }

    // Expects acct3 as head with: null <- acct3 <-> acct2 <-> acct -> null
    private void checkRemoveInteriorOfSizeThreeList(AbstractTRS trs, Address contract, Address acct,
        Address acct2, Address acct3) {

        assertEquals(acct3, getLinkedListHead(trs, contract));
        assertEquals(acct2, getLinkedListNext(trs, contract, acct3));
        assertEquals(acct, getLinkedListNext(trs, contract, acct2));
        assertNull(getLinkedListNext(trs, contract, acct));
        assertEquals(acct2, getLinkedListPrev(trs, contract, acct));
        assertEquals(acct3, getLinkedListPrev(trs, contract, acct2));
        assertNull(getLinkedListPrev(trs, contract, acct3));

        // We remove acct2. Should have:      null <- acct3 <-> acct -> null    with acct3 as head.
        byte[] input = getRefundInput(contract, acct2, DEFAULT_BALANCE);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());

        assertFalse(accountIsValid(trs, contract, acct2));
        assertEquals(acct3, getLinkedListHead(trs, contract));
        assertEquals(acct, getLinkedListNext(trs, contract, acct3));
        assertNull(getLinkedListNext(trs, contract, acct));
        assertEquals(acct3, getLinkedListPrev(trs, contract, acct));
        assertNull(getLinkedListPrev(trs, contract, acct3));
    }

    private void checkRemoveInteriorOfLargerList(Address contract, Address owner, int listSize) {
        // We have a linked list with 10 depositors. Grab the 5th in line. Ensure each address is unique too.
        TRSuseContract trs = newTRSuseContract(owner);
        Address next = getLinkedListHead(trs, contract);
        Address head = new Address(next.toBytes());
        Address mid = null;
        Set<Address> addressesInList = new HashSet<>();
        for (int i = 0; i < listSize; i++) {
            if (i == listSize - 1) {
                assertNull(getLinkedListNext(trs, contract, next));
            } else if (i == 4) {
                next = getLinkedListNext(trs, contract, next);
                assertNotNull(next);
                assertFalse(addressesInList.contains(next));
                addressesInList.add(next);
                mid = new Address(next.toBytes());
            } else {
                next = getLinkedListNext(trs, contract, next);
                assertNotNull(next);
                assertFalse(addressesInList.contains(next));
                addressesInList.add(next);
            }
        }

        // Remove mid. Iterate over list again.
        byte[] input = getRefundInput(contract, mid, DEFAULT_BALANCE);
        assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());
        assertFalse(accountIsValid(trs, contract, mid));

        assertEquals(head, getLinkedListHead(trs, contract));
        for (int i = 0; i < listSize - 1; i++) {
            if (i == listSize - 2) {
                assertNull(getLinkedListNext(trs, contract, head));
            } else {
                head = getLinkedListNext(trs, contract, head);
                assertNotNull(head);
                assertTrue(addressesInList.contains(head));
                assertNotEquals(mid, head);
            }
        }
    }

    private void checkMultipleListRemovals(Address contract, Address owner, int listSize) {
        // We have a linked list with 10 depositors. Ensure each address is unique. Grab every other
        // address to remove.
        TRSuseContract trs = newTRSuseContract(owner);
        Address next = getLinkedListHead(trs, contract);
        Set<Address> removals = new HashSet<>();
        Set<Address> addressesInList = new HashSet<>();
        for (int i = 0; i < listSize; i++) {
            if (i == listSize - 1) {
                assertNull(getLinkedListNext(trs, contract, next));
            } else {
                next = getLinkedListNext(trs, contract, next);
                assertNotNull(next);
                assertFalse(addressesInList.contains(next));
                addressesInList.add(next);
                if (i % 2 == 0) {
                    removals.add(next);
                }
            }
        }

        // Remove all accts in removals. Iterate over list again.
        for (Address rm : removals) {
            byte[] input = getRefundInput(contract, rm, DEFAULT_BALANCE);
            assertEquals(ResultCode.SUCCESS, trs.execute(input, COST).getResultCode());
            assertFalse(accountIsValid(trs, contract, rm));
        }

        // Note: may give +/-1 errors if listSize is not divisible by 2.
        Address head = getLinkedListHead(trs, contract);
        assertFalse(removals.contains(head));
        for (int i = 0; i < listSize / 2; i++) {
            if (i == (listSize / 2) - 1) {
                assertNull(getLinkedListNext(trs, contract, head));
            } else {
                head = getLinkedListNext(trs, contract, head);
                assertNotNull(head);
                assertTrue(addressesInList.contains(head));
                assertFalse(removals.contains(head));
            }
        }
    }

}
