package org.aion.tutorials;

import org.aion.api.IAionAPI;
import org.aion.api.IContract;
import org.aion.api.IUtils;
import org.aion.api.sol.IAddress;
import org.aion.api.sol.ISString;
import org.aion.api.type.*;
import org.aion.base.type.Address;
import org.aion.base.type.Hash256;

import java.util.Map;
import java.util.stream.Stream;

import static org.aion.api.ITx.NRG_LIMIT_CONTRACT_CREATE_MAX;
import static org.aion.api.ITx.NRG_LIMIT_TX_MAX;
import static org.aion.api.ITx.NRG_PRICE_MIN;

public class ContractExample {

    public static void main(String[] args) throws InterruptedException {

        // connect to Java API
        IAionAPI api = IAionAPI.init();
        ApiMsg apiMsg = api.connect(IAionAPI.LOCALHOST_URL);

        // failed connection
        if (apiMsg.isError()) {
            System.out.format("Could not connect due to <%s>%n", apiMsg.getErrString());
            System.exit(-1);
        }

        //        // 1. eth_getCompilers
        //        // not available at present
        //
        //        // 2. eth_compileSolidity
        //
        //        // contract source code
        //        String contract = "contract ticker { uint public val; function tick () { val+= 1; } }";
        //
        //        // compile code
        //        ApiMsg result = api.getTx().compile(contract);
        //
        //        // print result
        //        System.out.println(result.getObject().toString());
        //
        //        // 3. eth_gasPrice
        //
        //        // get NRG price
        //        long price = api.getTx().getNrgPrice().getObject();
        //
        //        // print price
        //        System.out.println("current NRG price = " + price + " nAmp");
        //
        //        // 4. eth_estimateGas
        //
        //        // compile code
        //        Map<String, CompileResponse> result = api.getTx()
        //                .compile("contract ticker { uint public val; function tick () { val+= 1; }
        // }").getObject();
        //
        //        // get NRG estimate
        //        long estimate =
        // api.getTx().estimateNrg(result.get("ticker").getCode()).getObject();
        //
        //        // print estimate
        //        System.out.println("NRG estimate for contract = " + estimate + " NRG");
        //
        //        // prepare transaction
        //        TxArgs tx = new TxArgs.TxArgsBuilder()
        //
        // .from(Address.wrap("a0bd0ef93902d9e123521a67bef7391e9487e963b2346ef3b3ff78208835545e"))
        //
        // .to(Address.wrap("a06f02e986965ddd3398c4de87e3708072ad58d96e9c53e87c31c8c970b211e5"))
        //                .value(BigInteger.TEN).createTxArgs();
        //
        //        // get NRG estimate
        //        estimate = api.getTx().estimateNrg(tx).getObject();
        //
        //        // print estimate
        //        System.out.println("NRG estimate for transaction = " + estimate + " NRG");
        //
        //        // 5.a) deploy contract
        //
        //        // contract source code
        //        String contractSource = "contract ticker { uint public val; function tick () { val+= 1; } }";
        //
        //        // compile code
        //        Map<String, CompileResponse> result = api.getTx().compile(contractSource).getObject();
        //        CompileResponse contract = result.get("ticker");
        //
        //        // unlock owner
        //        Address owner = Address.wrap("0xa0bd0ef93902d9e123521a67bef7391e9487e963b2346ef3b3ff78208835545e");
        //        boolean isUnlocked = api.getWallet().unlockAccount(owner, "password", 100).getObject();
        //        System.out.format("owner account %s%n", isUnlocked ? "unlocked" : "locked");
        //
        //        // deploy contract
        //        ContractDeploy.ContractDeployBuilder builder = new ContractDeploy.ContractDeployBuilder()
        //                .compileResponse(contract).value(BigInteger.ZERO).nrgPrice(NRG_PRICE_MIN)
        //                .nrgLimit(NRG_LIMIT_CONTRACT_CREATE_MAX).from(owner).data(ByteArrayWrapper.wrap(Bytesable.NULL_BYTE));
        //
        //        DeployResponse contractResponse = api.getTx().contractDeploy(builder.createContractDeploy()).getObject();
        //
        //        // print response
        //        Hash256 txHash = contractResponse.getTxid();
        //        Address contractAccount = contractResponse.getAddress();
        //        System.out.format("%ntransaction hash:%n\t%s%ncontract address: %n\t%s%n",
        //                          txHash.toString(),
        //                          contractAccount.toString());
        //
        //        // get & print receipt
        //        TxReceipt txReceipt = api.getTx().getTxReceipt(txHash).getObject();
        //        System.out.format("%ntransaction receipt:%n%s%n", txReceipt.toString());
        //
//                // 5.b) deploy contract alternative
//
//                // contract source code
//                String contractSource =
//                        "contract Personnel { address public owner; modifier onlyOwner() { require(msg.sender == owner); _;} "
//                                + "mapping(bytes32 => address) private userList; /** 3 LSB bits for each privilege type */ "
//                                + "mapping(address => bytes1) private userPrivilege; function Personnel(){ owner = msg.sender; } "
//                                + "event UserAdded(string _stamp); event AddressAdded(address indexed _addr); "
//                                + "function getUserAddress(string _stamp) constant returns (address){ return userList[sha3(_stamp)]; } "
//                                + "function addUser(string _stamp, address _addr, bytes1 _userPrivilege) "
//                                + "onlyOwner{ userList[sha3(_stamp)] = _addr; userPrivilege[_addr] = _userPrivilege; "
//                                + "UserAdded(_stamp); } function addAddress(string _stamp, address _addr) "
//                                + "onlyOwner{ userList[sha3(_stamp)] = _addr; AddressAdded(_addr); } }";
//
//                // unlock owner
//                Address owner = Address.wrap("0xa0bd0ef93902d9e123521a67bef7391e9487e963b2346ef3b3ff78208835545e");
//                boolean isUnlocked = api.getWallet().unlockAccount(owner, "Pufu1ete", 100).getObject();
//                System.out.format("owner account %s%n", isUnlocked ? "unlocked" : "locked");
//
//                // clear old deploy
//                api.getContractController().clear();
//
//                // deploy contract
//                ApiMsg msg = api.getContractController()
//                        .createFromSource(contractSource, owner, NRG_LIMIT_CONTRACT_CREATE_MAX, NRG_PRICE_MIN);
//
//                if (msg.isError()) {
//                    System.out.println("deploy contract failed! " + msg.getErrString());
//                } else {
//                    // get contract
//                    IContract contractResponse = api.getContractController().getContract();
//
//                    // print response
//                    Hash256 txHash = contractResponse.getDeployTxId();
//                    Address contractAccount = contractResponse.getContractAddress();
//                    System.out.format("%ntransaction hash:%n\t%s%ncontract address: %n\t%s%n",
//                                      txHash.toString(),
//                                      contractAccount.toString());
//
//                    // get & print receipt
//                    TxReceipt txReceipt = api.getTx().getTxReceipt(txHash).getObject();
//                    System.out.format("%ntransaction receipt:%n%s%n", txReceipt.toString());
//                }
        //
        //        // 6. eth_getCode
        //
        //        // set contract account
        //        Address contractAccount = Address.wrap("a0960fcb7d6423a0446243916c7c6360543b3d2f9c5e1c5ff7badb472b782b79");
        //
        //        // get code from latest block
        //        long blockNumber = -1L; // code for latest
        //        byte[] code = api.getTx().getCode(contractAccount, blockNumber).getObject();
        //
        //        // print code
        //        System.out.println("0x" + IUtils.bytes2Hex(code));
//
//        // 7. eth_getStorageAt
//
//        // set contract account
//        Address contractAccount = Address.wrap("a0960fcb7d6423a0446243916c7c6360543b3d2f9c5e1c5ff7badb472b782b79");
//
//        // get value from storage
//        long blockNumber = -1L; // code for latest
//        String value = api.getChain().getStorageAt(contractAccount, 0, blockNumber).getObject();
//
//        // print value
//        System.out.println(value);
//
//        // 8. eth_call

        Hash256 txHash = Hash256.wrap("0xb42a5f995450531f66e7db40efdfad2c310fa0f8dbca2a88c31fdc4837368e48");
        TxReceipt txReceipt = api.getTx().getTxReceipt(txHash).getObject();
        System.out.format("%ntransaction receipt:%n%s%n", txReceipt.toString());

        // set contract account
        Address contractAccount = txReceipt.getContractAddress();
        Address ownerAddress = txReceipt.getFrom();

        for (int i = 0; i < 10; i++) {
            // get value from storage
            long blockNumber = -1L; // code for latest
            String value = api.getChain().getStorageAt(contractAccount, i, blockNumber).getObject();

            // print value
            System.out.println(value);
        }

        String contractSource =
                "contract Personnel { address public owner; modifier onlyOwner() { require(msg.sender == owner); _;} "
                        + "mapping(bytes32 => address) private userList; /** 3 LSB bits for each privilege type */ "
                        + "mapping(address => bytes1) private userPrivilege; function Personnel(){ owner = msg.sender; } "
                        + "event UserAdded(string _stamp); event AddressAdded(address indexed _addr); "
                        + "function getUserAddress(string _stamp) constant returns (address){ return userList[sha3(_stamp)]; } "
                        + "function addUser(string _stamp, address _addr, bytes1 _userPrivilege) "
                        + "onlyOwner{ userList[sha3(_stamp)] = _addr; userPrivilege[_addr] = _userPrivilege; "
                        + "UserAdded(_stamp); } function addAddress(string _stamp, address _addr) "
                        + "onlyOwner{ userList[sha3(_stamp)] = _addr; AddressAdded(_addr); } }";

        // compile code
        Map<String, CompileResponse> result = api.getTx().compile(contractSource).getObject();
        CompileResponse contract = result.get("Personnel");

        IContract ctr = api.getContractController().getContractAt(ownerAddress, contractAccount, contract.getAbiDefString());

        System.out.println(ctr);

        api.getWallet().unlockAccount(ownerAddress, "Pufu1ete");

//        ApiMsg cr = ctr.newFunction("addAddress")
//                .setFrom(ownerAddress)
//                .setParam(ISString.copyFrom("jd-3"))
//                .setParam(IAddress.copyFrom(contractAccount.toString()))
//                .setTxNrgPrice(NRG_PRICE_MIN)
//                .setTxNrgLimit(NRG_LIMIT_TX_MAX)
//                .build()
//                .execute();
//
//        if (cr.isError()) {
//            System.out.println(cr.getErrString());
//        }


        Thread.sleep(50000L);
     ContractResponse   cr = ctr.newFunction("getUserAddress")
                .setParam(ISString.copyFrom("jd-3"))
                .setTxNrgLimit(NRG_LIMIT_TX_MAX)
                .setTxNrgPrice(NRG_PRICE_MIN)
                .build()
                .execute()
                .getObject();

        if(!IUtils.bytes2Hex((byte[])cr.getData().get(0)).equals(contractAccount.toString()))
            System.out.println("error occurred");
else {
    System.out.println(IUtils.bytes2Hex((byte[])cr.getData().get(0)));
        }
//        Object obj = cr.getObject();


        for (int i = 0; i < 10; i++) {
            // get value from storage
            long blockNumber = -1L; // code for latest
            String value = api.getChain().getStorageAt(contractAccount, i, blockNumber).getObject();

            // print value
            System.out.println(value);
        }



        // disconnect from api
        api.destroyApi();

        System.exit(0);
    }
}
