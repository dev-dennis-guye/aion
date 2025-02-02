package org.aion.zero.impl.db;

import static com.google.common.truth.Truth.assertThat;
import static org.aion.zero.impl.db.DatabaseUtils.deleteRecursively;
import static org.aion.p2p.P2pConstant.LARGE_REQUEST_SIZE;
import static org.aion.p2p.P2pConstant.STEP_COUNT;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.aion.db.impl.DBVendor;
import org.aion.db.impl.DatabaseFactory.Props;
import org.aion.log.AionLoggerFactory;
import org.aion.mcf.blockchain.Block;
import org.aion.mcf.db.exception.InvalidFilePathException;
import org.aion.util.TestResources;
import org.aion.util.types.ByteArrayWrapper;
import org.aion.zero.impl.types.AionBlock;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author Alexandra Roatis */
public class PendingBlockStoreTest {
    @BeforeClass
    public static void setup() {
        // logging to see errors
        Map<String, String> cfg = new HashMap<>();
        cfg.put("DB", "INFO");

        AionLoggerFactory.init(cfg);
    }

    @Test
    public void testConstructor_wMockDB() {
        Properties props = new Properties();
        props.setProperty(Props.DB_TYPE, DBVendor.MOCKDB.toValue());

        PendingBlockStore pb = null;
        try {
            pb = new PendingBlockStore(props);
        } catch (InvalidFilePathException e) {
            e.printStackTrace();
        }
        assertThat(pb.isOpen()).isTrue();
    }

    @Test(expected = Exception.class)
    public void testConstructor_woVendor() throws Exception {
        Properties props = new Properties();
        new PendingBlockStore(props);
    }

    @Test(expected = Exception.class)
    public void testConstructor_woPathAndName() throws Exception {
        Properties props = new Properties();
        props.setProperty(Props.DB_TYPE, DBVendor.PERSISTENTMOCKDB.toValue());
        new PendingBlockStore(props);
    }

    @Test
    public void testConstructor_wPersistentDB() {
        File dir = new File(System.getProperty("user.dir"), "tmp-" + System.currentTimeMillis());

        Properties props = new Properties();
        props.setProperty(Props.DB_TYPE, DBVendor.PERSISTENTMOCKDB.toValue());
        props.setProperty(Props.DB_PATH, dir.getAbsolutePath());
        props.setProperty(Props.DB_NAME, "pbTest");

        PendingBlockStore pb = null;
        try {
            pb = new PendingBlockStore(props);
        } catch (InvalidFilePathException e) {
            e.printStackTrace();
        }
        assertThat(pb.isOpen()).isTrue();

        List<Block> blocks = TestResources.consecutiveBlocks(6);
        assertThat(blocks.size()).isEqualTo(6);

        // test with valid block
        Block block = blocks.remove(0);
        assertThat(pb.addStatusBlock(block)).isTrue();
        // #index=1 #level=1 #queue=1 #status=1
        assertThat(pb.getIndexSize()).isEqualTo(1);
        assertThat(pb.getLevelSize()).isEqualTo(1);
        assertThat(pb.getQueueSize()).isEqualTo(1);
        assertThat(pb.getStatusSize()).isEqualTo(1);
        assertThat(pb.getStatusItem(block.getHash())).isNotNull();

        // test with valid range
        List<Block> range = new ArrayList<>();
        int rangeSize = 4;
        for (int i = 0; i < rangeSize; i++) {
            range.add(blocks.remove(0));
        }
        assertThat(pb.addBlockRange(range)).isEqualTo(rangeSize);
        // #index=5 #level=2 #queue=2 #status=1
        assertThat(pb.getIndexSize()).isEqualTo(5);
        assertThat(pb.getLevelSize()).isEqualTo(2);
        assertThat(pb.getQueueSize()).isEqualTo(2);
        assertThat(pb.getStatusSize()).isEqualTo(1);

        // test with valid block expanding range
        block = blocks.remove(0);
        assertThat(pb.addStatusBlock(block)).isTrue();
        // #index=6 #level=2 #queue=2 #status=2
        assertThat(pb.getIndexSize()).isEqualTo(6);
        assertThat(pb.getLevelSize()).isEqualTo(2);
        assertThat(pb.getQueueSize()).isEqualTo(2);
        assertThat(pb.getStatusSize()).isEqualTo(2);
        assertThat(pb.getStatusItem(block.getHash())).isNull();
        assertThat(pb.getStatusItem(range.get(0).getHash())).isNotNull();

        // close
        pb.close();
        assertThat(pb.isOpen()).isFalse();

        // check persistence of storage
        try {
            pb = new PendingBlockStore(props);
        } catch (InvalidFilePathException e) {
            e.printStackTrace();
        }
        assertThat(pb.isOpen()).isTrue();

        assertThat(pb.getIndexSize()).isEqualTo(6);
        assertThat(pb.getLevelSize()).isEqualTo(2);
        assertThat(pb.getQueueSize()).isEqualTo(2);
        assertThat(pb.getStatusSize()).isEqualTo(0);

        pb.close();

        assertThat(deleteRecursively(dir)).isTrue();
    }

    @Test
    public void testAddStatusBlock() {
        Properties props = new Properties();
        props.setProperty(Props.DB_TYPE, DBVendor.MOCKDB.toValue());

        PendingBlockStore pb = null;
        try {
            pb = new PendingBlockStore(props);
        } catch (InvalidFilePathException e) {
            e.printStackTrace();
        }
        assertThat(pb.isOpen()).isTrue();

        List<Block> blocks = TestResources.consecutiveBlocks(4);
        assertThat(blocks.size()).isEqualTo(4);

        // test with null input
        assertThat(pb.addStatusBlock(null)).isFalse();
        // #index=0 #level=0 #queue=0 #status=0
        assertThat(pb.getIndexSize()).isEqualTo(0);
        assertThat(pb.getLevelSize()).isEqualTo(0);
        assertThat(pb.getQueueSize()).isEqualTo(0);
        assertThat(pb.getStatusSize()).isEqualTo(0);
        assertThat(pb.getStatusItem(null)).isNull();

        // test with valid block
        Block block = blocks.get(0);
        assertThat(pb.addStatusBlock(block)).isTrue();
        // #index=1 #level=1 #queue=1 #status=1
        assertThat(pb.getIndexSize()).isEqualTo(1);
        assertThat(pb.getLevelSize()).isEqualTo(1);
        assertThat(pb.getQueueSize()).isEqualTo(1);
        assertThat(pb.getStatusSize()).isEqualTo(1);
        assertThat(pb.getStatusItem(block.getHash())).isNotNull();

        // test that block does not get added twice
        assertThat(pb.addStatusBlock(block)).isFalse();
        // #index=1 #level=1 #queue=1 #status=1
        assertThat(pb.getIndexSize()).isEqualTo(1);
        assertThat(pb.getLevelSize()).isEqualTo(1);
        assertThat(pb.getQueueSize()).isEqualTo(1);
        assertThat(pb.getStatusSize()).isEqualTo(1);

        // expand existing queue
        block = blocks.get(1);
        assertThat(pb.addStatusBlock(block)).isTrue();
        // #index=2 #level=1 #queue=1 #status=1
        assertThat(pb.getIndexSize()).isEqualTo(2);
        assertThat(pb.getLevelSize()).isEqualTo(1);
        assertThat(pb.getQueueSize()).isEqualTo(1);
        assertThat(pb.getStatusSize()).isEqualTo(1);
        assertThat(pb.getStatusItem(block.getHash())).isNull();

        // create new queue
        block = blocks.get(3);
        assertThat(pb.addStatusBlock(block)).isTrue();
        // #index=3 #level=2 #queue=2 #status=2
        assertThat(pb.getIndexSize()).isEqualTo(3);
        assertThat(pb.getLevelSize()).isEqualTo(2);
        assertThat(pb.getQueueSize()).isEqualTo(2);
        assertThat(pb.getStatusSize()).isEqualTo(2);
        assertThat(pb.getStatusItem(block.getHash())).isNotNull();

        // expand previous existing queue
        block = blocks.get(2);
        assertThat(pb.addStatusBlock(block)).isTrue();
        // #index=4 #level=2 #queue=2 #status=2
        assertThat(pb.getIndexSize()).isEqualTo(4);
        assertThat(pb.getLevelSize()).isEqualTo(2);
        assertThat(pb.getQueueSize()).isEqualTo(2);
        assertThat(pb.getStatusSize()).isEqualTo(2);
        assertThat(pb.getStatusItem(block.getHash())).isNull();
    }

    @Test
    public void testAddStatusBlock_wException() {
        Properties props = new Properties();
        props.setProperty(Props.DB_TYPE, DBVendor.MOCKDB.toValue());

        PendingBlockStore pb = null;
        try {
            pb = new PendingBlockStore(props);
        } catch (InvalidFilePathException e) {
            e.printStackTrace();
        }
        assertThat(pb.isOpen()).isTrue();

        Block block = TestResources.consecutiveBlocks(1).get(0);

        // closing the pending block store to cause exception
        pb.close();

        assertThat(pb.addStatusBlock(block)).isFalse();
    }

    @Test
    public void testAddBlockRange() {
        Properties props = new Properties();
        props.setProperty(Props.DB_TYPE, DBVendor.MOCKDB.toValue());

        PendingBlockStore pb = null;
        try {
            pb = new PendingBlockStore(props);
        } catch (InvalidFilePathException e) {
            e.printStackTrace();
        }
        assertThat(pb.isOpen()).isTrue();

        List<Block> blocks = TestResources.consecutiveBlocks(16);
        assertThat(blocks.size()).isEqualTo(16);

        // test with empty list input
        List<Block> input = new ArrayList<>();
        assertThat(pb.addBlockRange(input)).isEqualTo(0); // #index=0 #level=0 #queue=0
        assertThat(pb.getStatusSize()).isEqualTo(0);

        // test with valid range
        int rangeSize = 4;
        for (int i = 0; i < rangeSize; i++) {
            input.add(blocks.remove(0));
        }
        assertThat(pb.addBlockRange(input)).isEqualTo(rangeSize);
        // #index=4 #level=1 #queue=1 #status=0
        assertThat(pb.getIndexSize()).isEqualTo(rangeSize);
        assertThat(pb.getLevelSize()).isEqualTo(1);
        assertThat(pb.getQueueSize()).isEqualTo(1);
        assertThat(pb.getStatusSize()).isEqualTo(0);

        // test that the block range does not get added twice
        assertThat(pb.addBlockRange(input)).isEqualTo(0);
        // #index=4 #level=1 #queue=1 #status=0
        assertThat(pb.getIndexSize()).isEqualTo(rangeSize);
        assertThat(pb.getLevelSize()).isEqualTo(1);
        assertThat(pb.getQueueSize()).isEqualTo(1);
        assertThat(pb.getStatusSize()).isEqualTo(0);

        // add new queue though expand existing queue is possible
        // this is done for performance when deleting
        input.clear();
        for (int i = 0; i < rangeSize; i++) {
            input.add(blocks.remove(0));
        }
        assertThat(pb.addBlockRange(input)).isEqualTo(rangeSize);
        // #index=8 #level=2 #queue=2 #status=0
        assertThat(pb.getIndexSize()).isEqualTo(rangeSize * 2);
        assertThat(pb.getLevelSize()).isEqualTo(2);
        assertThat(pb.getQueueSize()).isEqualTo(2);
        assertThat(pb.getStatusSize()).isEqualTo(0);

        // create new queue
        input.clear();
        for (int i = 0; i < rangeSize; i++) {
            // skips one of the elements
            input.add(blocks.remove(2));
        }
        assertThat(pb.addBlockRange(input)).isEqualTo(rangeSize);
        // #index=12 #level=3 #queue=3 #status=0
        assertThat(pb.getIndexSize()).isEqualTo(rangeSize * 3);
        assertThat(pb.getLevelSize()).isEqualTo(3);
        assertThat(pb.getQueueSize()).isEqualTo(3);
        assertThat(pb.getStatusSize()).isEqualTo(0);

        // non-consecutive range -> 2 new queues
        input.clear();
        for (int i = 0; i < rangeSize; i++) {
            input.add(blocks.remove(0));
        }
        assertThat(pb.addBlockRange(input)).isEqualTo(rangeSize);
        // #index=16 #level=5 #queue=5 #status=0
        assertThat(pb.getIndexSize()).isEqualTo(rangeSize * 4);
        assertThat(pb.getLevelSize()).isEqualTo(5);
        assertThat(pb.getQueueSize()).isEqualTo(5);
        assertThat(pb.getStatusSize()).isEqualTo(0);
    }

    @Test
    public void testAddBlockRange_wException() {
        Properties props = new Properties();
        props.setProperty(Props.DB_TYPE, DBVendor.MOCKDB.toValue());

        PendingBlockStore pb = null;
        try {
            pb = new PendingBlockStore(props);
        } catch (InvalidFilePathException e) {
            e.printStackTrace();
        }
        assertThat(pb.isOpen()).isTrue();

        List<Block> blocks = TestResources.consecutiveBlocks(4);
        assertThat(blocks.size()).isEqualTo(4);

        // closing the pending block store to cause exception
        pb.close();

        assertThat(pb.addBlockRange(blocks)).isEqualTo(0);
    }

    @Test
    public void testLoadBlockRange() {
        Properties props = new Properties();
        props.setProperty(Props.DB_TYPE, DBVendor.MOCKDB.toValue());

        PendingBlockStore pb = null;
        try {
            pb = new PendingBlockStore(props);
        } catch (InvalidFilePathException e) {
            e.printStackTrace();
        }
        assertThat(pb.isOpen()).isTrue();

        List<Block> allBlocks = TestResources.consecutiveBlocks(8);

        // 1. test with empty storage
        assertThat(pb.loadBlockRange(100)).isEmpty();

        // 2. test with valid range
        List<Block> blocks = allBlocks.subList(0, 6);
        Block first = blocks.get(0);
        assertThat(blocks.size()).isEqualTo(6);
        assertThat(pb.addBlockRange(blocks)).isEqualTo(6);
        Map<ByteArrayWrapper, List<Block>> actual = pb.loadBlockRange(first.getNumber());
        assertThat(actual.size()).isEqualTo(1);
        assertThat(actual.get(ByteArrayWrapper.wrap(first.getHash()))).isEqualTo(blocks);

        // 3. test with multiple queues

        // create side chain
        AionBlock altBlock = new AionBlock(first.getEncoded());
        altBlock.setExtraData("random".getBytes());
        assertThat(altBlock.equals(first)).isFalse();
        List<Block> sideChain = new ArrayList<>();
        sideChain.add(altBlock);
        assertThat(pb.addBlockRange(sideChain)).isEqualTo(1);

        // check functionality
        actual = pb.loadBlockRange(first.getNumber());
        assertThat(actual.size()).isEqualTo(2);
        assertThat(actual.get(ByteArrayWrapper.wrap(first.getHash()))).isEqualTo(blocks);
        assertThat(actual.get(ByteArrayWrapper.wrap(altBlock.getHash()))).isEqualTo(sideChain);

        // 4. test with empty level
        long level = first.getNumber();
        assertThat(pb.loadBlockRange(level - 1)).isEmpty();
        assertThat(pb.loadBlockRange(level + 1)).isEmpty();

        // 5. test after status import with new queue
        Block status = allBlocks.get(7);
        assertThat(pb.addStatusBlock(status)).isTrue();
        actual = pb.loadBlockRange(status.getNumber());
        assertThat(actual.size()).isEqualTo(1);
        assertThat(actual.get(ByteArrayWrapper.wrap(status.getHash())).get(0)).isEqualTo(status);
        level = status.getNumber();
        assertThat(pb.loadBlockRange(level - 1)).isEmpty();
        assertThat(pb.loadBlockRange(level + 1)).isEmpty();

        // 6.  test after status import with extended queue
        status = allBlocks.get(6);
        assertThat(pb.addStatusBlock(status)).isTrue();
        assertThat(pb.loadBlockRange(status.getNumber())).isEmpty();

        actual = pb.loadBlockRange(first.getNumber());
        blocks.add(status);
        assertThat(actual.get(ByteArrayWrapper.wrap(first.getHash()))).isEqualTo(blocks);
    }

    @Test
    public void testLoadBlockRange_wException() {
        Properties props = new Properties();
        props.setProperty(Props.DB_TYPE, DBVendor.MOCKDB.toValue());

        PendingBlockStore pb = null;
        try {
            pb = new PendingBlockStore(props);
        } catch (InvalidFilePathException e) {
            e.printStackTrace();
        }
        assertThat(pb.isOpen()).isTrue();

        // closing the pending block store to cause exception
        pb.close();

        assertThat(pb.loadBlockRange(100)).isEmpty();
    }

    @Test
    public void testDropPendingQueues() {
        Properties props = new Properties();
        props.setProperty(Props.DB_TYPE, DBVendor.MOCKDB.toValue());

        PendingBlockStore pb = null;
        try {
            pb = new PendingBlockStore(props);
        } catch (InvalidFilePathException e) {
            e.printStackTrace();
        }
        assertThat(pb.isOpen()).isTrue();

        // add first queue
        List<Block> blocks = TestResources.consecutiveBlocks(6);
        Block first = blocks.get(0);
        pb.addBlockRange(blocks);

        // add second queue
        AionBlock altBlock = new AionBlock(first.getEncoded());
        altBlock.setExtraData("random".getBytes());
        List<Block> sideChain = new ArrayList<>();
        sideChain.add(altBlock);
        pb.addBlockRange(sideChain);

        // check storage updates
        assertThat(pb.getIndexSize()).isEqualTo(7);
        assertThat(pb.getLevelSize()).isEqualTo(1);
        assertThat(pb.getQueueSize()).isEqualTo(2);
        assertThat(pb.getStatusSize()).isEqualTo(0);

        // test drop functionality
        Map<ByteArrayWrapper, List<Block>> actual = pb.loadBlockRange(first.getNumber());
        pb.dropPendingQueues(first.getNumber(), actual.keySet(), actual);

        // check storage after drop functionality
        assertThat(pb.getIndexSize()).isEqualTo(0);
        assertThat(pb.getLevelSize()).isEqualTo(0);
        assertThat(pb.getQueueSize()).isEqualTo(0);
        assertThat(pb.getStatusSize()).isEqualTo(0);
    }

    @Test
    public void testDropPendingQueues_wException() {
        Properties props = new Properties();
        props.setProperty(Props.DB_TYPE, DBVendor.MOCKDB.toValue());

        PendingBlockStore pb = null;
        try {
            pb = new PendingBlockStore(props);
        } catch (InvalidFilePathException e) {
            e.printStackTrace();
        }
        assertThat(pb.isOpen()).isTrue();

        // add first queue
        List<Block> blocks = TestResources.consecutiveBlocks(6);
        Block first = blocks.get(0);
        pb.addBlockRange(blocks);

        // add second queue
        AionBlock altBlock = new AionBlock(first.getEncoded());
        altBlock.setExtraData("random".getBytes());
        List<Block> sideChain = new ArrayList<>();
        sideChain.add(altBlock);
        pb.addBlockRange(sideChain);

        // check storage updates
        assertThat(pb.getIndexSize()).isEqualTo(7);
        assertThat(pb.getLevelSize()).isEqualTo(1);
        assertThat(pb.getQueueSize()).isEqualTo(2);
        assertThat(pb.getStatusSize()).isEqualTo(0);

        // closing the pending block store to cause exception
        pb.close();

        // test drop functionality
        Map<ByteArrayWrapper, List<Block>> actual = pb.loadBlockRange(first.getNumber());
        pb.dropPendingQueues(first.getNumber(), actual.keySet(), actual);
    }

    @Test
    public void testDropPendingQueues_wSingleQueue() {
        Properties props = new Properties();
        props.setProperty(Props.DB_TYPE, DBVendor.MOCKDB.toValue());

        PendingBlockStore pb = null;
        try {
            pb = new PendingBlockStore(props);
        } catch (InvalidFilePathException e) {
            e.printStackTrace();
        }
        assertThat(pb.isOpen()).isTrue();

        // add first queue
        List<Block> blocks = TestResources.consecutiveBlocks(6);
        Block first = blocks.get(0);
        pb.addBlockRange(blocks);

        // add second queue
        AionBlock altBlock = new AionBlock(first.getEncoded());
        altBlock.setExtraData("random".getBytes());
        List<Block> sideChain = new ArrayList<>();
        sideChain.add(altBlock);
        pb.addBlockRange(sideChain);

        // check storage updates
        assertThat(pb.getIndexSize()).isEqualTo(7);
        assertThat(pb.getLevelSize()).isEqualTo(1);
        assertThat(pb.getQueueSize()).isEqualTo(2);
        assertThat(pb.getStatusSize()).isEqualTo(0);

        // test drop functionality
        Map<ByteArrayWrapper, List<Block>> actual = pb.loadBlockRange(first.getNumber());
        List<ByteArrayWrapper> queues = new ArrayList<>();
        queues.add(ByteArrayWrapper.wrap(first.getHash()));
        pb.dropPendingQueues(first.getNumber(), queues, actual);

        // check storage after drop functionality
        assertThat(pb.getIndexSize()).isEqualTo(1);
        assertThat(pb.getLevelSize()).isEqualTo(1);
        assertThat(pb.getQueueSize()).isEqualTo(1);
        assertThat(pb.getStatusSize()).isEqualTo(0);
    }

    @Test
    public void testDropPendingQueues_wSubsetOfQueueBlocks() {
        Properties props = new Properties();
        props.setProperty(Props.DB_TYPE, DBVendor.MOCKDB.toValue());

        PendingBlockStore pb = null;
        try {
            pb = new PendingBlockStore(props);
        } catch (InvalidFilePathException e) {
            e.printStackTrace();
        }
        assertThat(pb.isOpen()).isTrue();

        // add first queue
        List<Block> blocks = TestResources.consecutiveBlocks(6);
        Block first = blocks.get(0);
        pb.addBlockRange(blocks);

        // add second queue
        AionBlock altBlock = new AionBlock(first.getEncoded());
        altBlock.setExtraData("random".getBytes());
        List<Block> sideChain = new ArrayList<>();
        sideChain.add(altBlock);
        pb.addBlockRange(sideChain);

        // check storage updates
        assertThat(pb.getIndexSize()).isEqualTo(7);
        assertThat(pb.getLevelSize()).isEqualTo(1);
        assertThat(pb.getQueueSize()).isEqualTo(2);
        assertThat(pb.getStatusSize()).isEqualTo(0);

        // test drop functionality
        ByteArrayWrapper queueId = ByteArrayWrapper.wrap(first.getHash());
        List<ByteArrayWrapper> queues = new ArrayList<>();
        queues.add(queueId);
        Map<ByteArrayWrapper, List<Block>> actual = pb.loadBlockRange(first.getNumber());
        actual.get(queueId).remove(5);
        pb.dropPendingQueues(first.getNumber(), queues, actual);

        // check storage after drop functionality
        assertThat(pb.getIndexSize()).isEqualTo(2);
        assertThat(pb.getLevelSize()).isEqualTo(2);
        assertThat(pb.getQueueSize()).isEqualTo(2);
        assertThat(pb.getStatusSize()).isEqualTo(0);
    }

    @Test
    public void testNextBase_wException() {
        Properties props = new Properties();
        props.setProperty(Props.DB_TYPE, DBVendor.MOCKDB.toValue());

        PendingBlockStore pb = null;
        try {
            pb = new PendingBlockStore(props);
        } catch (InvalidFilePathException e) {
            e.printStackTrace();
        }
        assertThat(pb.isOpen()).isTrue();

        Block block = TestResources.consecutiveBlocks(1).get(0);

        // setup: add status
        assertThat(pb.addStatusBlock(block)).isTrue();
        assertThat(pb.getStatusSize()).isEqualTo(1);

        long current = block.getNumber() + 1; // above last status
        long knownBest = 0; // not important for this test
        long expected = current;

        // closing the pending block store to cause exception
        pb.close();

        assertThat(pb.nextBase(current, knownBest)).isEqualTo(expected);
    }

    @Test
    public void testNextBase_woStatus() {
        Properties props = new Properties();
        props.setProperty(Props.DB_TYPE, DBVendor.MOCKDB.toValue());

        PendingBlockStore pb = null;
        try {
            pb = new PendingBlockStore(props);
        } catch (InvalidFilePathException e) {
            e.printStackTrace();
        }
        assertThat(pb.isOpen()).isTrue();

        long current = 100L;
        long knownBest = 0L; // not yet known
        long expected = current + STEP_COUNT * LARGE_REQUEST_SIZE;
        assertThat(pb.nextBase(current, knownBest)).isEqualTo(expected);
    }

    @Test
    public void testNextBase_wSmallKnownBest() {
        Properties props = new Properties();
        props.setProperty(Props.DB_TYPE, DBVendor.MOCKDB.toValue());

        PendingBlockStore pb = null;
        try {
            pb = new PendingBlockStore(props);
        } catch (InvalidFilePathException e) {
            e.printStackTrace();
        }
        assertThat(pb.isOpen()).isTrue();

        long current = 100L;
        long knownBest = current + LARGE_REQUEST_SIZE - 1;
        long expected = current;
        assertThat(pb.nextBase(current, knownBest)).isEqualTo(expected);
    }

    @Test
    public void testNextBase_wSufficientKnownBest() {
        Properties props = new Properties();
        props.setProperty(Props.DB_TYPE, DBVendor.MOCKDB.toValue());

        PendingBlockStore pb = null;
        try {
            pb = new PendingBlockStore(props);
        } catch (InvalidFilePathException e) {
            e.printStackTrace();
        }
        assertThat(pb.isOpen()).isTrue();

        long current = 100L;
        long knownBest = current + LARGE_REQUEST_SIZE;
        long expected = current + STEP_COUNT * LARGE_REQUEST_SIZE;
        // current will be chosen
        assertThat(pb.nextBase(current, knownBest)).isEqualTo(expected);

        knownBest = expected + LARGE_REQUEST_SIZE;
        expected = expected + STEP_COUNT * LARGE_REQUEST_SIZE;
        // maxRequest will be chosen
        assertThat(pb.nextBase(current, knownBest)).isEqualTo(expected);
    }

    @Test
    public void testNextBase_wFailedStatusSearch() {
        Properties props = new Properties();
        props.setProperty(Props.DB_TYPE, DBVendor.MOCKDB.toValue());

        PendingBlockStore pb = null;
        try {
            pb = new PendingBlockStore(props);
        } catch (InvalidFilePathException e) {
            e.printStackTrace();
        }
        assertThat(pb.isOpen()).isTrue();

        Block block = TestResources.consecutiveBlocks(1).get(0);

        // setup: add status
        assertThat(pb.addStatusBlock(block)).isTrue();
        assertThat(pb.getStatusSize()).isEqualTo(1);

        long current = block.getNumber() + 1; // above last status
        long knownBest = 0; // not important for this test
        long expected = current;
        assertThat(pb.nextBase(current, knownBest)).isEqualTo(expected);
    }

    @Test
    public void testNextBase_wSuccessfulStatusSearch_wSingleEntry() {
        Properties props = new Properties();
        props.setProperty(Props.DB_TYPE, DBVendor.MOCKDB.toValue());

        PendingBlockStore pb = null;
        try {
            pb = new PendingBlockStore(props);
        } catch (InvalidFilePathException e) {
            e.printStackTrace();
        }
        assertThat(pb.isOpen()).isTrue();

        List<Block> blocks = TestResources.consecutiveBlocks(3);
        assertThat(blocks.size()).isEqualTo(3);

        // setup: add status entries
        for (Block block : blocks) {
            assertThat(pb.addStatusBlock(block)).isTrue();
            // all blocks get added to the same queue
            assertThat(pb.getStatusSize()).isEqualTo(1);
        }

        long current = blocks.get(0).getNumber() + 1; // above last status
        long knownBest = 0; // not important for this test
        long expected = blocks.get(2).getNumber() + 1;
        // last value from the status entry will be chosen
        assertThat(pb.nextBase(current, knownBest)).isEqualTo(expected);
    }

    @Test
    public void testNextBase_wSuccessfulStatusSearch_wTwoEntries() {
        Properties props = new Properties();
        props.setProperty(Props.DB_TYPE, DBVendor.MOCKDB.toValue());

        PendingBlockStore pb = null;
        try {
            pb = new PendingBlockStore(props);
        } catch (InvalidFilePathException e) {
            e.printStackTrace();
        }
        assertThat(pb.isOpen()).isTrue();

        List<Block> blocks = TestResources.consecutiveBlocks(3);

        // setup: add status entries
        Block status1 = blocks.get(0);
        assertThat(pb.addStatusBlock(status1)).isTrue();
        assertThat(pb.getStatusSize()).isEqualTo(1);

        Block status2 = blocks.get(2);
        assertThat(pb.addStatusBlock(status2)).isTrue();
        assertThat(pb.getStatusSize()).isEqualTo(2);

        long current = status1.getNumber() + 1; // above last status
        long knownBest = 0; // not important for this test
        long expected = status2.getNumber() + 1;
        // last value from the second status entry will be chosen
        assertThat(pb.nextBase(current, knownBest)).isEqualTo(expected);
    }
}
