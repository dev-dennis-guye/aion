package org.aion.db.impl;

import static com.google.common.truth.Truth.assertThat;
import static org.aion.db.impl.DatabaseFactory.Props.DB_NAME;
import static org.aion.db.impl.DatabaseFactory.Props.ENABLE_LOCKING;
import static org.aion.db.impl.DatabaseTestUtils.assertConcurrent;

import com.google.common.truth.Truth;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.aion.db.utils.FileUtils;
import org.aion.log.AionLoggerFactory;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author Alexandra Roatis */
@RunWith(JUnitParamsRunner.class)
public class ConcurrencyTest {

    private static final int CONCURRENT_THREADS = 200;
    private static final int TIME_OUT = 100; // in seconds
    private static final boolean DISPLAY_MESSAGES = false;
    public static final Logger log = LoggerFactory.getLogger("DB");

    private static int count = 0;

    private static synchronized int getNext() {
        count++;
        return count;
    }

    @BeforeClass
    public static void setup() {
        // logging to see errors
        Map<String, String> cfg = new HashMap<>();
        cfg.put("DB", "WARN");

        AionLoggerFactory.init(cfg);
    }

    @AfterClass
    public static void teardown() {
        // clean out the tmp directory
        Truth.assertThat(FileUtils.deleteRecursively(DatabaseTestUtils.testDir)).isTrue();
        Truth.assertThat(DatabaseTestUtils.testDir.mkdirs()).isTrue();
    }

    @Before
    public void deleteFromDisk() {
        // clean out the tmp directory
        assertThat(FileUtils.deleteRecursively(DatabaseTestUtils.testDir)).isTrue();
        Truth.assertThat(DatabaseTestUtils.testDir.mkdirs()).isTrue();
    }

    /** @return parameters for testing */
    @SuppressWarnings("unused")
    private Object databaseInstanceDefinitions() {
        return DatabaseTestUtils.unlockedDatabaseInstanceDefinitions();
    }

    private static int count(Iterator<byte[]> keys) {
        int size = 0;
        while (keys.hasNext()) {
            size++;
            keys.next();
        }
        return size;
    }

    private void addThread4IsEmpty(List<Runnable> threads, ByteArrayKeyValueDatabase db) {
        threads.add(
                () -> {
                    boolean check = db.isEmpty();
                    if (DISPLAY_MESSAGES) {
                        System.out.println(
                                Thread.currentThread().getName()
                                        + ": "
                                        + (check ? "EMPTY" : "NOT EMPTY"));
                    }
                });
    }

    private void addThread4Keys(List<Runnable> threads, ByteArrayKeyValueDatabase db) {
        threads.add(
                () -> {
                    Iterator<byte[]> keys = db.keys();
                    if (DISPLAY_MESSAGES) {
                        System.out.println(
                                Thread.currentThread().getName() + ": #keys = " + count(keys));
                    }
                });
    }

    private void addThread4Get(List<Runnable> threads, ByteArrayKeyValueDatabase db, String key) {
        threads.add(
                () -> {
                    boolean hasValue = db.get(key.getBytes()).isPresent();
                    if (DISPLAY_MESSAGES) {
                        System.out.println(
                                Thread.currentThread().getName()
                                        + ": "
                                        + key
                                        + " "
                                        + (hasValue ? "PRESENT" : "NOT PRESENT"));
                    }
                });
    }

    private void addThread4Put(List<Runnable> threads, ByteArrayKeyValueDatabase db, String key) {
        threads.add(
                () -> {
                    db.put(key.getBytes(), DatabaseTestUtils.randomBytes(32));
                    if (DISPLAY_MESSAGES) {
                        System.out.println(
                                Thread.currentThread().getName() + ": " + key + " ADDED");
                    }
                });
    }

    private void addThread4Delete(
            List<Runnable> threads, ByteArrayKeyValueDatabase db, String key) {
        threads.add(
                () -> {
                    db.delete(key.getBytes());
                    if (DISPLAY_MESSAGES) {
                        System.out.println(
                                Thread.currentThread().getName() + ": " + key + " DELETED");
                    }
                });
    }

    private void addThread4PutBatch(
            List<Runnable> threads, ByteArrayKeyValueDatabase db, String key) {
        threads.add(
                () -> {
                    Map<byte[], byte[]> map = new HashMap<>();
                    map.put((key + 1).getBytes(), DatabaseTestUtils.randomBytes(32));
                    map.put((key + 2).getBytes(), DatabaseTestUtils.randomBytes(32));
                    map.put((key + 3).getBytes(), DatabaseTestUtils.randomBytes(32));
                    db.putBatch(map);
                    if (DISPLAY_MESSAGES) {
                        System.out.println(
                                Thread.currentThread().getName()
                                        + ": "
                                        + (key + 1)
                                        + ", "
                                        + (key + 2)
                                        + ", "
                                        + (key + 3)
                                        + " ADDED");
                    }
                });
    }

    private void addThread4DeleteBatch(
            List<Runnable> threads, ByteArrayKeyValueDatabase db, String key) {
        threads.add(
                () -> {
                    List<byte[]> list = new ArrayList<>();
                    list.add((key + 1).getBytes());
                    list.add((key + 2).getBytes());
                    list.add((key + 3).getBytes());
                    db.deleteBatch(list);
                    if (DISPLAY_MESSAGES) {
                        System.out.println(
                                Thread.currentThread().getName()
                                        + ": "
                                        + (key + 1)
                                        + ", "
                                        + (key + 2)
                                        + ", "
                                        + (key + 3)
                                        + " DELETED");
                    }
                });
    }

    private void addThread4Open(List<Runnable> threads, ByteArrayKeyValueDatabase db) {
        threads.add(
                () -> {
                    db.open();
                    if (DISPLAY_MESSAGES) {
                        System.out.println(Thread.currentThread().getName() + ": OPENED");
                    }
                });
    }

    private void addThread4Close(List<Runnable> threads, ByteArrayKeyValueDatabase db) {
        threads.add(
                () -> {
                    db.close();
                    if (DISPLAY_MESSAGES) {
                        System.out.println(Thread.currentThread().getName() + ": CLOSED");
                    }
                });
    }

    private void addThread4Size(List<Runnable> threads, ByteArrayKeyValueDatabase db) {
        threads.add(
                () -> {
                    long size = db.approximateSize();
                    if (DISPLAY_MESSAGES) {
                        System.out.println(
                                Thread.currentThread().getName() + ": approx. size = " + size);
                    }
                });
    }

    @Test
    @Parameters(method = "databaseInstanceDefinitions")
    public void testConcurrentAccessOnOpenDatabase(Properties dbDef) throws InterruptedException {
        dbDef.setProperty(DB_NAME, DatabaseTestUtils.dbName + getNext());
        dbDef.setProperty(ENABLE_LOCKING, "true");
        // open database
        ByteArrayKeyValueDatabase db = DatabaseFactory.connect(dbDef, log);
        assertThat(db.open()).isTrue();

        // create distinct threads with
        List<Runnable> threads = new ArrayList<>();

        int threadSetCount = CONCURRENT_THREADS / 8;
        if (threadSetCount < 3) {
            threadSetCount = 3;
        }

        for (int i = 0; i < threadSetCount; i++) {
            // 1. thread that checks empty
            addThread4IsEmpty(threads, db);

            // 2. thread that gets keys
            addThread4Keys(threads, db);

            String keyStr = "key-" + i + ".";

            // 3. thread that gets entry
            addThread4Get(threads, db, keyStr);

            // 4. thread that puts entry
            addThread4Put(threads, db, keyStr);

            // 5. thread that deletes entry
            addThread4Delete(threads, db, keyStr);

            // 6. thread that puts entries
            addThread4PutBatch(threads, db, keyStr);

            // 7. thread that deletes entry
            addThread4DeleteBatch(threads, db, keyStr);

            // 8. thread that checks size
            addThread4Size(threads, db);
        }

        // run threads and check for exceptions
        assertConcurrent("Testing concurrent access. ", threads, TIME_OUT);

        // check that db is unlocked after updates
        assertThat(db.isLocked()).isFalse();

        // ensuring close
        db.close();
        assertThat(db.isClosed()).isTrue();
    }

    @Test
    @Parameters(method = "databaseInstanceDefinitions")
    public void testConcurrentPut(Properties dbDef) throws InterruptedException {
        dbDef.setProperty(DB_NAME, DatabaseTestUtils.dbName + getNext());
        dbDef.setProperty(ENABLE_LOCKING, "true");
        ByteArrayKeyValueDatabase db = DatabaseFactory.connect(dbDef, log);
        assertThat(db.open()).isTrue();

        // create distinct threads with
        List<Runnable> threads = new ArrayList<>();

        for (int i = 0; i < CONCURRENT_THREADS; i++) {
            addThread4Put(threads, db, "key-" + i);
        }

        // run threads
        assertConcurrent("Testing put(...) ", threads, TIME_OUT);

        // check that all values were added
        assertThat(count(db.keys())).isEqualTo(CONCURRENT_THREADS);

        // ensuring close
        db.close();
        assertThat(db.isClosed()).isTrue();
    }

    @Test
    @Parameters(method = "databaseInstanceDefinitions")
    public void testConcurrentPutBatch(Properties dbDef) throws InterruptedException {
        dbDef.setProperty(DB_NAME, DatabaseTestUtils.dbName + getNext());
        dbDef.setProperty(ENABLE_LOCKING, "true");
        ByteArrayKeyValueDatabase db = DatabaseFactory.connect(dbDef, log);
        assertThat(db.open()).isTrue();

        // create distinct threads with
        List<Runnable> threads = new ArrayList<>();

        for (int i = 0; i < CONCURRENT_THREADS; i++) {
            addThread4PutBatch(threads, db, "key-" + i);
        }

        // run threads
        assertConcurrent("Testing putBatch(...) ", threads, TIME_OUT);

        // check that all values were added
        assertThat(count(db.keys())).isEqualTo(3 * CONCURRENT_THREADS);

        // ensuring close
        db.close();
        assertThat(db.isClosed()).isTrue();
    }

    @Test
    @Parameters(method = "databaseInstanceDefinitions")
    public void testConcurrentUpdate(Properties dbDef) throws InterruptedException {
        dbDef.setProperty(DB_NAME, DatabaseTestUtils.dbName + getNext());
        dbDef.setProperty(ENABLE_LOCKING, "true");
        // open database
        ByteArrayKeyValueDatabase db = DatabaseFactory.connect(dbDef, log);
        assertThat(db.open()).isTrue();

        // create distinct threads with
        List<Runnable> threads = new ArrayList<>();

        int threadSetCount = CONCURRENT_THREADS / 4;
        if (threadSetCount < 3) {
            threadSetCount = 3;
        }

        for (int i = 0; i < threadSetCount; i++) {
            String keyStr = "key-" + i + ".";

            // 1. thread that puts entry
            addThread4Put(threads, db, keyStr);

            // 2. thread that deletes entry
            addThread4Delete(threads, db, keyStr);

            // 3. thread that puts entries
            addThread4PutBatch(threads, db, keyStr);

            // 4. thread that deletes entry
            addThread4DeleteBatch(threads, db, keyStr);
        }

        // run threads and check for exceptions
        assertConcurrent("Testing concurrent updates. ", threads, TIME_OUT);

        // check that db is unlocked after updates
        assertThat(db.isLocked()).isFalse();

        // ensuring close
        db.close();
        assertThat(db.isClosed()).isTrue();
    }
}
