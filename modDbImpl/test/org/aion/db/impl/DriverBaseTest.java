package org.aion.db.impl;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.common.truth.Truth;
import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.aion.db.generic.DatabaseWithCache;
import org.aion.db.generic.LockedDatabase;
import org.aion.db.impl.h2.H2MVMap;
import org.aion.db.impl.leveldb.LevelDB;
import org.aion.db.impl.mockdb.MockDB;
import org.aion.db.impl.mockdb.PersistentMockDB;
import org.aion.db.utils.FileUtils;
import org.aion.log.AionLoggerFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Unwritten Tests List:
 * - concurrent access and modification
 * - correct file creation and deletion
 * - disconnect and reconnect
 * - simultaneous connections to the same DB by different threads
 * - released locks after execution with exceptions
 */

/**
 * Base database tests, to be passed by all driver implementations.
 *
 * @author ali
 * @author Alexandra Roatis
 */
@RunWith(Parameterized.class)
public class DriverBaseTest {

    private static final File testDir = new File(System.getProperty("user.dir"), "tmp");
    private static final String dbNamePrefix = "TestDB";
    private static final String dbPath = testDir.getAbsolutePath();
    private static final String unboundHeapCache = "0";
    public static final Logger log = LoggerFactory.getLogger("DB");

    //    public static String boundHeapCache = "256";

    @Parameters(name = "{0}")
    public static Iterable<Object[]> data() throws NoSuchMethodException, SecurityException {
        return Arrays.asList(
                new Object[][] {
                    // H2MVMap wo. db cache wo. compression
                    {
                        "H2MVMap",
                        new boolean[] {false, false, false},
                        // { isLocked, isHeapCacheEnabled, isAutocommitEnabled }
                        H2MVMap.class.getDeclaredConstructor(
                                String.class, String.class, Logger.class, boolean.class, boolean.class),
                        new Object[] {
                            dbNamePrefix + DatabaseTestUtils.getNext(), dbPath, log, false, false
                        }
                    },
                    // H2MVMap w. db cache wo. compression
                    {
                        "H2MVMap+dbCache",
                        new boolean[] {false, false, false},
                        H2MVMap.class.getDeclaredConstructor(
                                String.class, String.class, Logger.class, boolean.class, boolean.class),
                        new Object[] {
                            dbNamePrefix + DatabaseTestUtils.getNext(), dbPath, log, true, false
                        }
                    },
                    // H2MVMap wo. db cache w. compression
                    {
                        "H2MVMap+compression",
                        new boolean[] {false, false, false},
                        H2MVMap.class.getDeclaredConstructor(
                                String.class, String.class, Logger.class, boolean.class, boolean.class),
                        new Object[] {
                            dbNamePrefix + DatabaseTestUtils.getNext(), dbPath, log, false, true
                        }
                    },
                    // H2MVMap w. db cache w. compression
                    {
                        "H2MVMap+dbCache+compression",
                        new boolean[] {false, false, false},
                        H2MVMap.class.getDeclaredConstructor(
                                String.class, String.class, Logger.class, boolean.class, boolean.class),
                        new Object[] {
                            dbNamePrefix + DatabaseTestUtils.getNext(), dbPath, log, true, true
                        }
                    },
                    // LevelDB wo. db cache wo. compression
                    {
                        "LevelDB",
                        new boolean[] {false, false, false},
                        LevelDB.class.getDeclaredConstructor(
                                String.class, String.class, Logger.class, boolean.class, boolean.class),
                        new Object[] {
                            dbNamePrefix + DatabaseTestUtils.getNext(), dbPath, log, false, false
                        }
                    },
                    // LevelDB w. db cache wo. compression
                    {
                        "LevelDB+dbCache",
                        new boolean[] {false, false, false},
                        LevelDB.class.getDeclaredConstructor(
                                String.class, String.class, Logger.class, boolean.class, boolean.class),
                        new Object[] {
                            dbNamePrefix + DatabaseTestUtils.getNext(), dbPath, log, true, false
                        }
                    },
                    // LevelDB wo. db cache w. compression
                    {
                        "LevelDB+compression",
                        new boolean[] {false, false, false},
                        LevelDB.class.getDeclaredConstructor(
                                String.class, String.class, Logger.class, boolean.class, boolean.class),
                        new Object[] {
                            dbNamePrefix + DatabaseTestUtils.getNext(), dbPath, log, false, true
                        }
                    },
                    // LevelDB w. db cache w. compression
                    {
                        "LevelDB+dbCache+compression",
                        new boolean[] {false, false, false},
                        LevelDB.class.getDeclaredConstructor(
                                String.class, String.class, Logger.class, boolean.class, boolean.class),
                        new Object[] {
                            dbNamePrefix + DatabaseTestUtils.getNext(), dbPath, log, true, true
                        }
                    },
                    // MockDB
                    {
                        "MockDB",
                        new boolean[] {false, false, false},
                        MockDB.class.getDeclaredConstructor(String.class, Logger.class),
                        new Object[] {dbNamePrefix, log}
                    },
                    // PersistentMockDB
                    {
                        "PersistentMockDB",
                        new boolean[] {false, false, false},
                        PersistentMockDB.class.getDeclaredConstructor(String.class, String.class, Logger.class),
                        new Object[] {dbNamePrefix + DatabaseTestUtils.getNext(), dbPath, log}
                    },
                    // H2MVMap
                    {
                        "H2MVMap+lock",
                        new boolean[] {true, false, false},
                        H2MVMap.class.getDeclaredConstructor(
                                String.class, String.class, Logger.class, boolean.class, boolean.class),
                        new Object[] {
                            dbNamePrefix + DatabaseTestUtils.getNext(), dbPath, log, false, false
                        }
                    },
                    // LevelDB wo. db cache wo. compression
                    {
                        "LevelDB+lock",
                        new boolean[] {true, false, false},
                        LevelDB.class.getDeclaredConstructor(
                                String.class, String.class, Logger.class, boolean.class, boolean.class),
                        new Object[] {
                            dbNamePrefix + DatabaseTestUtils.getNext(), dbPath, log, false, false
                        }
                    },
                    // MockDB
                    {
                        "MockDB+lock",
                        new boolean[] {true, false, false},
                        MockDB.class.getDeclaredConstructor(String.class, Logger.class),
                        new Object[] {dbNamePrefix, log}
                    },
                    // H2MVMap
                    {
                        "H2MVMap+heapCache",
                        new boolean[] {false, true, false},
                        H2MVMap.class.getDeclaredConstructor(
                                String.class, String.class, Logger.class, boolean.class, boolean.class),
                        new Object[] {
                            dbNamePrefix + DatabaseTestUtils.getNext(), dbPath, log, false, false
                        }
                    },
                    // LevelDB wo. db cache wo. compression
                    {
                        "LevelDB+heapCache",
                        new boolean[] {false, true, false},
                        LevelDB.class.getDeclaredConstructor(
                                String.class, String.class, Logger.class, boolean.class, boolean.class),
                        new Object[] {
                            dbNamePrefix + DatabaseTestUtils.getNext(), dbPath, log, false, false
                        }
                    },
                    // MockDB
                    {
                        "MockDB+heapCache",
                        new boolean[] {false, true, false},
                        MockDB.class.getDeclaredConstructor(String.class, Logger.class),
                        new Object[] {dbNamePrefix, log}
                    },
                    // H2MVMap
                    {
                        "H2MVMap+heapCache+lock",
                        new boolean[] {true, true, false},
                        H2MVMap.class.getDeclaredConstructor(
                                String.class, String.class, Logger.class, boolean.class, boolean.class),
                        new Object[] {
                            dbNamePrefix + DatabaseTestUtils.getNext(), dbPath, log, false, false
                        }
                    },
                    // LevelDB wo. db cache wo. compression
                    {
                        "LevelDB+heapCache+lock",
                        new boolean[] {true, true, false},
                        LevelDB.class.getDeclaredConstructor(
                                String.class, String.class, Logger.class, boolean.class, boolean.class),
                        new Object[] {
                            dbNamePrefix + DatabaseTestUtils.getNext(), dbPath, log, false, false
                        }
                    },
                    // MockDB
                    {
                        "MockDB+heapCache+lock",
                        new boolean[] {true, true, false},
                        MockDB.class.getDeclaredConstructor(String.class, Logger.class),
                        new Object[] {dbNamePrefix, log}
                    },
                    // H2MVMap
                    {
                        "H2MVMap+heapCache+autocommit",
                        new boolean[] {false, true, true},
                        H2MVMap.class.getDeclaredConstructor(
                                String.class, String.class, Logger.class, boolean.class, boolean.class),
                        new Object[] {
                            dbNamePrefix + DatabaseTestUtils.getNext(), dbPath, log, false, false
                        }
                    },
                    // LevelDB wo. db cache wo. compression
                    {
                        "LevelDB+heapCache+autocommit",
                        new boolean[] {false, true, true},
                        LevelDB.class.getDeclaredConstructor(
                                String.class, String.class, Logger.class, boolean.class, boolean.class),
                        new Object[] {
                            dbNamePrefix + DatabaseTestUtils.getNext(), dbPath, log, false, false
                        }
                    },
                    // MockDB
                    {
                        "MockDB+heapCache+autocommit",
                        new boolean[] {false, true, true},
                        MockDB.class.getDeclaredConstructor(String.class, Logger.class),
                        new Object[] {dbNamePrefix, log}
                    },
                    // TODO: [Task AJK-169] re-enable MongoDB tests
                    // {
                    //     "MongoDB",
                    //     new boolean[] {false, false, false},
                    //     MongoDB.class.getDeclaredConstructor(String.class, String.class),
                    //     new Object[] {
                    //         dbNamePrefix + DatabaseTestUtils.getNext(),
                    //         MongoTestRunner.inst().getConnectionString()
                    //     }
                    // },
                    // {
                    //     "MongoDB+lock",
                    //     new boolean[] {true, false, false},
                    //     MongoDB.class.getDeclaredConstructor(String.class, String.class),
                    //     new Object[] {
                    //         dbNamePrefix + DatabaseTestUtils.getNext(),
                    //         MongoTestRunner.inst().getConnectionString()
                    //     }
                    // },
                    // {
                    //     "MongoDB+heapCache",
                    //     new boolean[] {false, true, false},
                    //     MongoDB.class.getDeclaredConstructor(String.class, String.class),
                    //     new Object[] {
                    //         dbNamePrefix + DatabaseTestUtils.getNext(),
                    //         MongoTestRunner.inst().getConnectionString()
                    //     }
                    // },
                    // {
                    //     "MongoDB+heapCache+lock",
                    //     new boolean[] {true, true, false},
                    //     MongoDB.class.getDeclaredConstructor(String.class, String.class),
                    //     new Object[] {
                    //         dbNamePrefix + DatabaseTestUtils.getNext(),
                    //         MongoTestRunner.inst().getConnectionString()
                    //     }
                    // },
                    // {
                    //     "MongoDB+heapCache+autocommit",
                    //     new boolean[] {false, true, true},
                    //     MongoDB.class.getDeclaredConstructor(String.class, String.class),
                    //     new Object[] {
                    //         dbNamePrefix + DatabaseTestUtils.getNext(),
                    //         MongoTestRunner.inst().getConnectionString()
                    //     }
                    // },
                });
    }

    private ByteArrayKeyValueDatabase db;

    private final Constructor<ByteArrayKeyValueDatabase> constructor;
    private final Object[] args;
    private final String dbName;

    private static final byte[] k1 = "key1".getBytes();
    private static final byte[] v1 = "value1".getBytes();

    private static final byte[] k2 = "key2".getBytes();
    private static final byte[] v2 = "value2".getBytes();

    private static final byte[] k3 = "key3".getBytes();
    private static final byte[] v3 = "value3".getBytes();

    /** Every test invocation instantiates a new IByteArrayKeyValueDB */
    public DriverBaseTest(
            String testName,
            boolean[] props,
            Constructor<ByteArrayKeyValueDatabase> constructor,
            Object[] args)
            throws InstantiationException, IllegalAccessException, IllegalArgumentException,
                    InvocationTargetException {

        // logging to see errors
        Map<String, String> cfg = new HashMap<>();
        cfg.put("DB", "WARN");

        AionLoggerFactory.init(cfg);

        this.constructor = constructor;
        this.args = args;
        this.dbName = (String) args[0];
        this.db = constructor.newInstance(args);

        if (props[1]) {
            this.db = new DatabaseWithCache((AbstractDB) this.db, log, props[2], "0", false);
        }
        if (props[0]) {
            this.db = new LockedDatabase(this.db, log);
        }
    }

    @BeforeClass
    public static void setup() {
        // clean out the tmp directory
        Truth.assertThat(FileUtils.deleteRecursively(testDir)).isTrue();
        assertThat(testDir.mkdirs()).isTrue();
    }

    @AfterClass
    public static void teardown() {
        assertThat(testDir.delete()).isTrue();
    }

    @Before
    public void open() {
        assertThat(db).isNotNull();
        assertThat(db.isOpen()).isFalse();
        assertThat(db.isClosed()).isTrue();

        if (db.getPersistenceMethod() == PersistenceMethod.FILE_BASED) {
            assertThat(db.isCreatedOnDisk()).isFalse();
            assertThat(db.getPath().get()).isEqualTo(new File(dbPath, dbName).getAbsolutePath());
        }

        assertThat(db.isLocked()).isFalse();
        assertThat(db.getName().get()).isEqualTo(dbName);

        assertThat(db.open()).isTrue();

        // Drop the old db's info if there's any there
        db.drop();

        assertThat(db.isOpen()).isTrue();
        assertThat(db.isClosed()).isFalse();
        assertThat(db.isEmpty()).isTrue();

        if (db.getPersistenceMethod() == PersistenceMethod.FILE_BASED) {
            assertThat(db.isCreatedOnDisk()).isTrue();
            assertThat(db.getPath().get()).isEqualTo(new File(dbPath, dbName).getAbsolutePath());
        }

        assertThat(db.isLocked()).isFalse();
        assertThat(db.getName().get()).isEqualTo(dbName);
    }

    @After
    public void close() {
        assertThat(db).isNotNull();
        assertThat(db.isOpen()).isTrue();
        assertThat(db.isClosed()).isFalse();

        if (db.getPersistenceMethod() == PersistenceMethod.FILE_BASED) {
            assertThat(db.isCreatedOnDisk()).isTrue();
            assertThat(db.getPath().get()).isEqualTo(new File(dbPath, dbName).getAbsolutePath());
        } else if (db.getPersistenceMethod() == PersistenceMethod.DBMS) {
            // Drop the DB before closing the connection for DBMS systems
            db.drop();
            assertThat(db.isEmpty()).isTrue();
        }

        assertThat(db.isLocked()).isFalse();
        assertThat(db.getName().get()).isEqualTo(dbName);

        db.close();

        assertThat(db.isOpen()).isFalse();
        assertThat(db.isClosed()).isTrue();

        // for non-persistent DB's, close() should wipe the DB
        if (db.getPersistenceMethod() == PersistenceMethod.FILE_BASED) {
            assertThat(db.isCreatedOnDisk()).isTrue();
            assertThat(FileUtils.deleteRecursively(new File(db.getPath().get()))).isTrue();
            assertThat(db.isCreatedOnDisk()).isFalse();
            assertThat(db.getPath().get()).isEqualTo(new File(dbPath, dbName).getAbsolutePath());
        }

        assertThat(db.isLocked()).isFalse();
        assertThat(db.getName().get()).isEqualTo(dbName);
    }

    private static int count(Iterator<byte[]> keys) {
        int size = 0;
        while (keys.hasNext()) {
            size++;
            keys.next();
        }
        return size;
    }

    @Test
    public void testConcurrentAccess() {
        // TODO: import test from legacy test case
    }

    @Test
    public void testOpenSecondInstance()
            throws InstantiationException, IllegalAccessException, IllegalArgumentException,
                    InvocationTargetException {
        if (db.getPersistenceMethod() == PersistenceMethod.FILE_BASED
                && !(db instanceof PersistentMockDB)) {
            // another connection to same DB should fail on open for all persistent KVDBs
            ByteArrayKeyValueDatabase otherDatabase = this.constructor.newInstance(this.args);
            assertThat(otherDatabase.open()).isFalse();

            // ensuring that new connection did not somehow close old one
            assertThat(db.isOpen()).isTrue();
            assertThat(db.isLocked()).isFalse();
        }
    }

    @Test
    public void testPersistence() throws InterruptedException {
        if (db.getPersistenceMethod() != PersistenceMethod.IN_MEMORY) {
            // adding data
            // ---------------------------------------------------------------------------------------------
            assertThat(db.get(k1).isPresent()).isFalse();
            db.put(k1, v1);
            assertThat(db.isLocked()).isFalse();

            // commit, close & reopen
            if (!db.isAutoCommitEnabled()) {
                db.commit();
            }

            db.close();
            Thread.sleep(100);

            assertThat(db.isClosed()).isTrue();
            assertThat(db.open()).isTrue();

            // ensure persistence
            assertThat(db.get(k1).get()).isEqualTo(v1);
            assertThat(db.isEmpty()).isFalse();
            assertThat(count(db.keys())).isEqualTo(1);
            assertThat(db.isLocked()).isFalse();

            // deleting data
            // -------------------------------------------------------------------------------------------
            db.delete(k1);
            assertThat(db.isLocked()).isFalse();

            // commit, close & reopen
            if (!db.isAutoCommitEnabled()) {
                db.commit();
            }

            db.close();
            Thread.sleep(100);

            assertThat(db.isClosed()).isTrue();
            assertThat(db.open()).isTrue();

            // ensure absence
            assertThat(db.get(k1).isPresent()).isFalse();
            assertThat(db.isEmpty()).isTrue();
            assertThat(db.keys().hasNext()).isFalse();
            assertThat(db.isLocked()).isFalse();
        }
    }

    @Test
    public void testBatchPersistence() throws InterruptedException {
        if (db.getPersistenceMethod() != PersistenceMethod.IN_MEMORY) {
            // adding data
            // ---------------------------------------------------------------------------------------------
            assertThat(db.get(k1).isPresent()).isFalse();
            assertThat(db.get(k2).isPresent()).isFalse();
            assertThat(db.get(k3).isPresent()).isFalse();

            Map<byte[], byte[]> map = new HashMap<>();
            map.put(k1, v1);
            map.put(k2, v2);
            map.put(k3, v3);
            db.putBatch(map);

            assertThat(db.isLocked()).isFalse();

            // commit, close & reopen
            if (!db.isAutoCommitEnabled()) {
                db.commit();
            }

            db.close();
            Thread.sleep(100);

            assertThat(db.isClosed()).isTrue();
            assertThat(db.open()).isTrue();

            // ensure persistence
            assertThat(db.get(k1).get()).isEqualTo(v1);
            assertThat(db.get(k2).get()).isEqualTo(v2);
            assertThat(db.get(k3).get()).isEqualTo(v3);
            assertThat(db.isEmpty()).isFalse();
            assertThat(count(db.keys())).isEqualTo(3);
            assertThat(db.isLocked()).isFalse();

            // updating data
            // -------------------------------------------------------------------------------------------
            map.clear();
            map.put(k1, v2);
            map.put(k2, v3);
            db.putBatch(map);

            List<byte[]> del = new ArrayList<>();
            del.add(k3);
            db.deleteBatch(del);

            assertThat(db.isLocked()).isFalse();

            // commit, close & reopen
            if (!db.isAutoCommitEnabled()) {
                db.commit();
            }

            db.close();
            Thread.sleep(100);

            assertThat(db.isClosed()).isTrue();
            assertThat(db.open()).isTrue();

            // ensure absence
            assertThat(db.get(k1).get()).isEqualTo(v2);
            assertThat(db.get(k2).get()).isEqualTo(v3);
            assertThat(db.get(k3).isPresent()).isFalse();
            assertThat(db.isEmpty()).isFalse();
            assertThat(count(db.keys())).isEqualTo(2);
            assertThat(db.isLocked()).isFalse();

            // deleting data
            // -------------------------------------------------------------------------------------------
            db.deleteBatch(map.keySet());

            assertThat(db.isLocked()).isFalse();

            // commit, close & reopen
            if (!db.isAutoCommitEnabled()) {
                db.commit();
            }

            db.close();
            Thread.sleep(100);

            assertThat(db.isClosed()).isTrue();
            assertThat(db.open()).isTrue();

            // ensure absence
            assertThat(db.get(k1).isPresent()).isFalse();
            assertThat(db.get(k2).isPresent()).isFalse();
            assertThat(db.get(k3).isPresent()).isFalse();
            assertThat(db.isEmpty()).isTrue();
            assertThat(db.keys().hasNext()).isFalse();
            assertThat(db.isLocked()).isFalse();
        }
    }

    @Test
    public void testPut() {
        assertThat(db.get(k1).isPresent()).isFalse();

        db.put(k1, v1);

        assertThat(db.get(k1).get()).isEqualTo(v1);

        // ensure unlocked
        assertThat(db.isLocked()).isFalse();
    }

    @Test
    public void testPutBatch() {
        assertThat(db.get(k1).isPresent()).isFalse();
        assertThat(db.get(k2).isPresent()).isFalse();

        Map<byte[], byte[]> map = new HashMap<>();
        map.put(k1, v1);
        map.put(k2, v2);
        db.putBatch(map);

        assertThat(v1).isEqualTo(db.get(k1).get());
        assertThat(v2).isEqualTo(db.get(k2).get());

        // ensure unlocked
        assertThat(db.isLocked()).isFalse();
    }

    @Test
    public void testUpdate() {
        // ensure existence
        assertThat(db.get(k1).isPresent()).isFalse();
        db.put(k1, v1);

        assertThat(v1).isEqualTo(db.get(k1).get());

        // check after update
        db.put(k1, v2);

        assertThat(v2).isEqualTo(db.get(k1).get());

        // check after direct delete
        db.delete(k1);

        assertThat(db.get(k1).isPresent()).isFalse();

        // ensure unlocked
        assertThat(db.isLocked()).isFalse();
    }

    @Test
    public void testUpdateBatch() {
        // ensure existence
        assertThat(db.get(k1).isPresent()).isFalse();
        assertThat(db.get(k2).isPresent()).isFalse();
        assertThat(db.get(k3).isPresent()).isFalse();
        db.put(k1, v1);
        db.put(k2, v2);

        assertThat(v1).isEqualTo(db.get(k1).get());
        assertThat(v2).isEqualTo(db.get(k2).get());

        // check after update
        Map<byte[], byte[]> ops = new HashMap<>();
        ops.put(k2, v1);
        ops.put(k3, v3);
        db.putBatch(ops);

        List<byte[]> del = new ArrayList<>();
        del.add(k1);
        db.deleteBatch(del);

        assertThat(db.get(k1).isPresent()).isFalse();
        assertThat(v1).isEqualTo(db.get(k2).get());
        assertThat(v3).isEqualTo(db.get(k3).get());

        // ensure unlocked
        assertThat(db.isLocked()).isFalse();
    }

    @Test
    public void testDelete() {
        // ensure existence
        db.put(k1, v1);

        assertThat(db.get(k1).isPresent()).isTrue();

        // check presence after delete
        db.delete(k1);

        assertThat(db.get(k1).isPresent()).isFalse();

        // ensure unlocked
        assertThat(db.isLocked()).isFalse();
    }

    @Test
    public void testDeleteBatch() {
        // ensure existence
        Map<byte[], byte[]> map = new HashMap<>();
        map.put(k1, v1);
        map.put(k2, v2);
        db.putBatch(map);

        List<byte[]> del = new ArrayList<>();
        del.add(k3);
        db.deleteBatch(del);

        assertThat(db.get(k1).isPresent()).isTrue();
        assertThat(db.get(k2).isPresent()).isTrue();
        assertThat(db.get(k3).isPresent()).isFalse();

        // check presence after delete
        db.deleteBatch(map.keySet());

        assertThat(db.get(k1).isPresent()).isFalse();
        assertThat(db.get(k2).isPresent()).isFalse();
        assertThat(db.get(k3).isPresent()).isFalse();

        // ensure unlocked
        assertThat(db.isLocked()).isFalse();
    }

    @Test
    public void testDrop() {
        // ensure existence
        Map<byte[], byte[]> map = new HashMap<>();
        map.put(k1, v1);
        map.put(k2, v2);
        db.putBatch(map);

        List<byte[]> del = new ArrayList<>();
        del.add(k3);
        db.deleteBatch(del);

        assertThat(db.get(k1).isPresent()).isTrue();
        assertThat(db.get(k2).isPresent()).isTrue();
        assertThat(db.get(k3).isPresent()).isFalse();

        // check presence after delete
        db.drop();

        assertThat(db.get(k1).isPresent()).isFalse();
        assertThat(db.get(k2).isPresent()).isFalse();
        assertThat(db.get(k3).isPresent()).isFalse();

        // ensure unlocked
        assertThat(db.isLocked()).isFalse();
    }

    @Test
    @Ignore
    /** This test is non-deterministic and may fail. If it does, re-run the test suite. */
    public void testApproximateDBSize() {
        if (db.getPersistenceMethod() == PersistenceMethod.FILE_BASED) {
            int repeat = 1_000_000;
            for (int i = 0; i < repeat; i++) {
                db.put(String.format("%c%09d", 'a' + i % 26, i).getBytes(), "test".getBytes());
            }
            // estimate
            long est = db.approximateSize();
            long count = FileUtils.getDirectorySizeBytes(db.getPath().get());

            double error = Math.abs(1.0 * (est - count) / count);
            assertTrue(error < 0.6);
        } else {
            assertTrue(db.approximateSize() == -1L);
        }
    }

    @Test
    public void testKeys() {
        // keys shouldn't be null even when empty
        Iterator<byte[]> keys = db.keys();
        assertThat(db.isLocked()).isFalse();
        assertThat(db.isEmpty()).isTrue();
        assertThat(keys).isNotNull();
        assertThat(keys.hasNext()).isFalse();

        // checking after put
        db.put(k1, v1);
        db.put(k2, v2);
        assertThat(db.get(k1).get()).isEqualTo(v1);
        assertThat(db.get(k2).get()).isEqualTo(v2);

        keys = db.keys();
        assertThat(db.isLocked()).isFalse();

        // because of byte[], set.contains() does not work as expected
        int countIn = 0, countOut = 0;
        while (keys.hasNext()) {
            byte[] k = keys.next();
            if (Arrays.equals(k1, k) || Arrays.equals(k2, k)) {
                countIn++;
            } else {
                countOut++;
            }
        }
        assertThat(countIn).isEqualTo(2);
        assertThat(countOut).isEqualTo(0);

        // checking after delete
        db.delete(k2);
        assertThat(db.get(k2).isPresent()).isFalse();

        keys = db.keys();
        assertThat(db.isLocked()).isFalse();

        countIn = 0;
        countOut = 0;
        while (keys.hasNext()) {
            byte[] k = keys.next();
            if (Arrays.equals(k1, k)) {
                countIn++;
            } else {
                countOut++;
            }
        }
        assertThat(countIn).isEqualTo(1);
        assertThat(countOut).isEqualTo(0);

        // checking after putBatch
        Map<byte[], byte[]> ops = new HashMap<>();
        ops.put(k2, v2);
        ops.put(k3, v3);
        db.putBatch(ops);

        List<byte[]> del = new ArrayList<>();
        del.add(k1);
        db.deleteBatch(del);

        keys = db.keys();
        assertThat(db.isLocked()).isFalse();

        countIn = 0;
        countOut = 0;
        while (keys.hasNext()) {
            byte[] k = keys.next();
            if (Arrays.equals(k2, k) || Arrays.equals(k3, k)) {
                countIn++;
            } else {
                countOut++;
            }
        }
        assertThat(countIn).isEqualTo(2);
        assertThat(countOut).isEqualTo(0);

        // checking after deleteBatch
        db.deleteBatch(ops.keySet());

        keys = db.keys();
        assertThat(db.isLocked()).isFalse();
        assertThat(keys.hasNext()).isFalse();
    }

    @Test
    public void testIsEmpty() {
        assertThat(db.isEmpty()).isTrue();
        assertThat(db.isLocked()).isFalse();

        // checking after put
        db.put(k1, v1);
        db.put(k2, v2);
        assertThat(db.get(k1).get()).isEqualTo(v1);
        assertThat(db.get(k2).get()).isEqualTo(v2);

        assertThat(db.isEmpty()).isFalse();
        assertThat(db.isLocked()).isFalse();

        // checking after delete
        db.delete(k2);
        assertThat(db.get(k2).isPresent()).isFalse();

        assertThat(db.isEmpty()).isFalse();
        assertThat(db.isLocked()).isFalse();

        db.delete(k1);

        assertThat(db.isEmpty()).isTrue();
        assertThat(db.isLocked()).isFalse();

        // checking after putBatch
        Map<byte[], byte[]> ops = new HashMap<>();
        ops.put(k2, v2);
        ops.put(k3, v3);
        db.putBatch(ops);

        List<byte[]> del = new ArrayList<>();
        del.add(k1);
        db.deleteBatch(del);

        assertThat(db.isEmpty()).isFalse();
        assertThat(db.isLocked()).isFalse();

        // checking after deleteBatch
        db.deleteBatch(ops.keySet());

        assertThat(db.isEmpty()).isTrue();
        assertThat(db.isLocked()).isFalse();
    }

    /** Checks that data does not persist without explicit commits. */
    @Test
    public void testAutoCommitDisabled() throws InterruptedException {
        if (db.getPersistenceMethod() != PersistenceMethod.IN_MEMORY && !db.isAutoCommitEnabled()) {
            // adding data
            // ---------------------------------------------------------------------------------------------
            assertThat(db.get(k1).isPresent()).isFalse();
            db.put(k1, v1);
            assertThat(db.isLocked()).isFalse();

            db.close();
            Thread.sleep(100);

            assertThat(db.isClosed()).isTrue();
            assertThat(db.open()).isTrue();

            // ensure lack of persistence
            assertThat(db.get(k1).isPresent()).isFalse();
            assertThat(db.isEmpty()).isTrue();
            assertThat(db.keys().hasNext()).isFalse();
            assertThat(db.isLocked()).isFalse();

            // deleting data
            // -------------------------------------------------------------------------------------------
            db.put(k1, v1);
            db.commit();
            assertThat(db.isLocked()).isFalse();

            db.delete(k1);
            assertThat(db.isLocked()).isFalse();

            db.close();
            Thread.sleep(100);

            assertThat(db.isClosed()).isTrue();
            assertThat(db.open()).isTrue();

            // ensure lack of persistence of delete
            assertThat(db.get(k1).get()).isEqualTo(v1);
            assertThat(db.isEmpty()).isFalse();
            assertThat(count(db.keys())).isEqualTo(1);
            assertThat(db.isLocked()).isFalse();

            // batch update
            // --------------------------------------------------------------------------------------------
            Map<byte[], byte[]> map = new HashMap<>();
            map.put(k2, v2);
            map.put(k3, v3);
            db.putBatch(map);

            List<byte[]> del = new ArrayList<>();
            del.add(k1);
            db.deleteBatch(del);

            db.commit();
            assertThat(db.isLocked()).isFalse();

            map.clear();
            map.put(k1, v2);
            map.put(k2, v3);
            db.putBatch(map);

            del = new ArrayList<>();
            del.add(k3);
            db.deleteBatch(del);

            assertThat(db.isLocked()).isFalse();

            db.close();
            Thread.sleep(100);

            assertThat(db.isClosed()).isTrue();
            assertThat(db.open()).isTrue();

            // ensure lack of persistence of second update
            assertThat(db.get(k1).isPresent()).isFalse();
            assertThat(db.get(k2).get()).isEqualTo(v2);
            assertThat(db.get(k3).get()).isEqualTo(v3);
            assertThat(db.isEmpty()).isFalse();
            assertThat(count(db.keys())).isEqualTo(2);
            assertThat(db.isLocked()).isFalse();

            // batch delete
            // --------------------------------------------------------------------------------------------
            db.deleteBatch(map.keySet());
            assertThat(db.isLocked()).isFalse();

            db.close();
            Thread.sleep(100);

            assertThat(db.isClosed()).isTrue();
            assertThat(db.open()).isTrue();

            // ensure lack of persistence of batch delete
            assertThat(db.get(k1).isPresent()).isFalse();
            assertThat(db.get(k2).get()).isEqualTo(v2);
            assertThat(db.get(k3).get()).isEqualTo(v3);
            assertThat(db.isEmpty()).isFalse();
            assertThat(count(db.keys())).isEqualTo(2);
            assertThat(db.isLocked()).isFalse();
        }
    }
}
