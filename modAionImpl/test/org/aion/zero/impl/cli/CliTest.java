package org.aion.zero.impl.cli;

import static com.google.common.truth.Truth.assertThat;
import static org.aion.util.TestResources.TEST_RESOURCE_DIR;
import static org.aion.zero.impl.cli.Cli.ReturnType.ERROR;
import static org.aion.zero.impl.cli.Cli.ReturnType.EXIT;
import static org.aion.zero.impl.cli.Cli.ReturnType.RUN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Objects;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.aion.crypto.ECKey;
import org.aion.crypto.ECKeyFac;
import org.aion.log.LogEnum;
import org.aion.log.LogLevel;
import org.aion.zero.impl.keystore.Keystore;
import org.aion.mcf.config.Cfg;
import org.aion.mcf.config.CfgDb;
import org.aion.util.conversions.Hex;
import org.aion.zero.impl.cli.Cli.ReturnType;
import org.aion.zero.impl.cli.Cli.TaskPriority;
import org.aion.zero.impl.config.CfgAion;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParseResult;

/** CliTest for new version with use of different networks. */
@RunWith(JUnitParamsRunner.class)
public class CliTest {

    private final PasswordReader mockpr = Mockito.mock(PasswordReader.class);
    private final Cli cli = new Cli(mockpr);
    private final CfgAion cfg = CfgAion.inst();
    private final Cli mockCli = spy(cli);

    // base paths
    private static final String BASE_PATH = System.getProperty("user.dir");
    private static final File MAIN_BASE_PATH = new File(BASE_PATH, "mainnet");
    private static final File TEST_BASE_PATH = new File(BASE_PATH, "mastery");
    private static final File AVM_TEST_BASE_PATH = new File(BASE_PATH, "avmtestnet");

    // config paths
    private static final File CONFIG_PATH = new File(BASE_PATH, "config");
    private static final File MAIN_CONFIG_PATH = new File(CONFIG_PATH, "mainnet");
    private static final File TEST_CONFIG_PATH = new File(CONFIG_PATH, "mastery");
    private static final File AVM_TEST_CONFIG_PATH = new File(CONFIG_PATH, "avmtestnet");

    private static final String module = "modAionImpl";

    private static final String configFileName = "config.xml";
    private static final String genesisFileName = "genesis.json";
    private static final String forkFileName = "fork.properties";

    private static final String dataDirectory = "datadir";
    private static final String alternativeDirectory = "random";

    private static final File path = new File(BASE_PATH, dataDirectory);
    private static final File alternativePath = new File(BASE_PATH, alternativeDirectory);

    private static final File config = new File(TEST_RESOURCE_DIR, configFileName);
    private static final File oldConfig = new File(CONFIG_PATH, configFileName);
    private static final File mainnetConfig = new File(MAIN_CONFIG_PATH, configFileName);
    private static final File testnetConfig = new File(TEST_CONFIG_PATH, configFileName);
    private static final File avmtestnetConfig = new File(AVM_TEST_CONFIG_PATH, configFileName);

    private static final File genesis = new File(TEST_RESOURCE_DIR, genesisFileName);
    private static final File oldGenesis = new File(CONFIG_PATH, genesisFileName);
    private static final File mainnetGenesis = new File(MAIN_CONFIG_PATH, genesisFileName);
    private static final File testnetGenesis = new File(TEST_CONFIG_PATH, genesisFileName);
    private static final File avmtestnetGenesis = new File(AVM_TEST_CONFIG_PATH, genesisFileName);

    private static final File fork = new File(TEST_RESOURCE_DIR, forkFileName);

    private static final File mainnetFork = new File(MAIN_CONFIG_PATH, forkFileName);
    private static final File testnetFork = new File(TEST_CONFIG_PATH, forkFileName);
    private static final File avmtestnetFork = new File(AVM_TEST_CONFIG_PATH, forkFileName);

    private static final String DEFAULT_PORT = "30303";
    private static final String TEST_PORT = "12345";
    private static final String INVALID_PORT = "123450";

    private static final int SLOW_IMPORT_TIME = 1_000; // 1 sec
    private static final int COMPACT_FREQUENCY = 600_000; // 10 min
    /** @implNote set this to true to enable printing */
    private static final boolean verbose = false;

    @Before
    public void setup() {
        // reset config values
        cfg.fromXML(config);

        if (BASE_PATH.contains(module) && !mainnetConfig.exists()) {
            // save config to disk at expected location for new kernel
            if (!MAIN_CONFIG_PATH.exists()) {
                assertThat(MAIN_CONFIG_PATH.mkdirs()).isTrue();
            }
            Cli.copyRecursively(config, mainnetConfig);
            Cli.copyRecursively(genesis, mainnetGenesis);
            Cli.copyRecursively(fork, mainnetFork);
        }

        if (BASE_PATH.contains(module) && !testnetConfig.exists()) {
            // save config to disk at expected location for new kernel
            if (!TEST_CONFIG_PATH.exists()) {
                assertThat(TEST_CONFIG_PATH.mkdirs()).isTrue();
            }
            Cli.copyRecursively(config, testnetConfig);
            Cli.copyRecursively(genesis, testnetGenesis);
            Cli.copyRecursively(fork, testnetFork);
        }

        if (BASE_PATH.contains(module) && !avmtestnetConfig.exists()) {
            // save config to disk at expected location for new kernel
            if (!AVM_TEST_CONFIG_PATH.exists()) {
                assertThat(AVM_TEST_CONFIG_PATH.mkdir()).isTrue();
            }
            Cli.copyRecursively(config, avmtestnetConfig);
            Cli.copyRecursively(genesis, avmtestnetGenesis);
            Cli.copyRecursively(fork, avmtestnetFork);
        }
        cfg.resetInternal();
        doReturn("password").when(mockpr).readPassword(any(), any());
        doCallRealMethod().when(mockCli).call(any(), any());
    }

    @After
    public void shutdown() {
        deleteRecursively(path);
        deleteRecursively(alternativePath);

        // in case absolute paths are used
        deleteRecursively(cfg.getKeystoreDir());
        deleteRecursively(cfg.getDatabaseDir());
        deleteRecursively(cfg.getLogDir());

        // to avoid deleting config for all tests
        if (BASE_PATH.contains(module)) {
            deleteRecursively(CONFIG_PATH);
        }

        deleteRecursively(MAIN_BASE_PATH);
        deleteRecursively(TEST_BASE_PATH);
        deleteRecursively(AVM_TEST_BASE_PATH);
    }

    @AfterClass
    public static void resetKeystorePath() {
        Keystore.initKeystorePath(); // reset the keystore path we've changed in the tests
    }

    /**
     * Ensures that the { <i>-h</i>, <i>--help</i>, <i>-v</i>, <i>--version</i> } arguments work.
     */
    @Test
    @Parameters({"-h", "--help", "-v", "--version"})
    public void testHelpAndVersion(String option) {
        assertThat(cli.call(new String[] {option}, cfg)).isEqualTo(EXIT);
    }

    /** Parameters for testing {@link #testDirectoryAndNetwork(String[], ReturnType, String)}. */
    @SuppressWarnings("unused")
    private Object parametersWithDirectoryAndNetwork() {
        List<Object> parameters = new ArrayList<>();

        String[] dir_options = new String[] {"-d", "--datadir"};
        String[] net_options = new String[] {"-n", "--network"};
        String expected = new File(path, "mainnet").getAbsolutePath();
        String expOnError = MAIN_BASE_PATH.getAbsolutePath();

        // data directory alone
        for (String op : dir_options) {
            // with relative path
            parameters.add(new Object[] {new String[] {op, dataDirectory}, RUN, expected});
            // with absolute path
            parameters.add(new Object[] {new String[] {op, path.getAbsolutePath()}, RUN, expected});
            // without value
            parameters.add(new Object[] {new String[] {op}, ERROR, expOnError});
            // with invalid characters (Linux & Win)
            parameters.add(new Object[] {new String[] {op, "/\\<>:\"|?*"}, ERROR, expOnError});
        }

        // network and directory
        String[] net_values = new String[] {"mainnet", "invalid"};
        for (String opDir : dir_options) {
            for (String opNet : net_options) {
                for (String valNet : net_values) {
                    // with relative path
                    parameters.add(
                            new Object[] {
                                new String[] {opDir, dataDirectory, opNet, valNet}, RUN, expected
                            });
                    parameters.add(
                            new Object[] {
                                new String[] {opNet, valNet, opDir, dataDirectory}, RUN, expected
                            });
                    // with absolute path
                    parameters.add(
                            new Object[] {
                                new String[] {opDir, path.getAbsolutePath(), opNet, valNet},
                                RUN,
                                expected
                            });
                    parameters.add(
                            new Object[] {
                                new String[] {opNet, valNet, opDir, path.getAbsolutePath()},
                                RUN,
                                expected
                            });
                }
            }
        }

        // network alone
        expected = MAIN_BASE_PATH.getAbsolutePath();
        for (String op : net_options) {
            // without parameter
            parameters.add(new Object[] {new String[] {op}, ERROR, expOnError});
            // with two parameters
            parameters.add(
                    new Object[] {new String[] {op, "testnet", "mastery"}, ERROR, expOnError});
            // invalid parameter
            parameters.add(new Object[] {new String[] {op, "invalid"}, RUN, expected});
            // mainnet as parameter
            parameters.add(new Object[] {new String[] {op, "mainnet"}, RUN, expected});
        }

        // network alone with testnet
        net_values = new String[] {"mastery", "testnet"};
        expected = TEST_BASE_PATH.getAbsolutePath();
        for (String op : net_options) {
            for (String netVal : net_values) {
                // mastery as parameter
                parameters.add(new Object[] {new String[] {op, netVal}, RUN, expected});
            }
        }

        // network alone with avmtestnet
        expected = AVM_TEST_BASE_PATH.getAbsolutePath();
        for (String op : net_options) {
            // avmtestnet as parameter
            parameters.add(new Object[] {new String[] {op, "avmtestnet"}, RUN, expected});
        }

        // network and directory with testnet
        expected = new File(path, "mastery").getAbsolutePath();
        for (String opDir : dir_options) {
            for (String opNet : net_options) {
                for (String netVal : net_values) {
                    // with relative path
                    parameters.add(
                            new Object[] {
                                new String[] {opDir, dataDirectory, opNet, netVal}, RUN, expected
                            });
                    parameters.add(
                            new Object[] {
                                new String[] {opNet, netVal, opDir, dataDirectory}, RUN, expected
                            });
                    // with absolute path
                    parameters.add(
                            new Object[] {
                                new String[] {opDir, path.getAbsolutePath(), opNet, netVal},
                                RUN,
                                expected
                            });
                    parameters.add(
                            new Object[] {
                                new String[] {opNet, netVal, opDir, path.getAbsolutePath()},
                                RUN,
                                expected
                            });
                }
            }
        }

        // with subdirectories
        String dir = dataDirectory + File.separator + "subfolder";
        File path = new File(BASE_PATH, dir);
        expected = new File(path, "mainnet").getAbsolutePath();
        for (String op : dir_options) {
            // with relative path with subdirectories
            parameters.add(new Object[] {new String[] {op, dir}, RUN, expected});
            // with multiple values
            parameters.add(new Object[] {new String[] {op, dataDirectory, dir}, ERROR, expOnError});
        }

        return parameters.toArray();
    }

    /**
     * Ensures that the { <i>-d</i>, <i>--datadir</i>, <i>-n</i>, <i>--network</i> } arguments work.
     */
    @Test
    @Parameters(method = "parametersWithDirectoryAndNetwork")
    public void testDirectoryAndNetwork(
            String[] input, ReturnType expectedReturn, String expectedPath) {
        assertThat(cli.call(input, cfg)).isEqualTo(expectedReturn);
        assertThat(cfg.getBasePath()).isEqualTo(expectedPath);
        assertThat(cfg.getExecConfigFile())
                .isEqualTo(new File(expectedPath, "config" + File.separator + configFileName));
        assertThat(cfg.getExecGenesisFile())
                .isEqualTo(new File(expectedPath, "config" + File.separator + genesisFileName));
        assertThat(cfg.getExecForkFile())
                .isEqualTo(new File(expectedPath, "config" + File.separator + forkFileName));
        assertThat(cfg.getDatabaseDir()).isEqualTo(new File(expectedPath, "database"));
        assertThat(cfg.getLogDir()).isEqualTo(new File(expectedPath, "log"));
        assertThat(cfg.getKeystoreDir()).isEqualTo(new File(expectedPath, "keystore"));

        if (verbose) {
            printPaths(cfg);
        }
    }

    private void printPaths(Cfg cfg) {
        System.out.println(
                "\n-------------------------------- USED PATHS --------------------------------"
                        + "\n> Logger path:   "
                        + cfg.getLogDir().getAbsolutePath()
                        + "\n> Database path: "
                        + cfg.getDatabaseDir().getAbsolutePath()
                        + "\n> Keystore path: "
                        + cfg.getKeystoreDir().getAbsolutePath()
                        + "\n> Config write:  "
                        + cfg.getExecConfigFile().getAbsolutePath()
                        + "\n> Genesis write: "
                        + cfg.getExecGenesisFile().getAbsolutePath()
                        + "\n> Fork write: "
                        + cfg.getExecForkFile().getAbsolutePath()
                        + "\n----------------------------------------------------------------------------"
                        + "\n> Config read:   "
                        + cfg.getInitialConfigFile().getAbsolutePath()
                        + "\n> Genesis read:  "
                        + cfg.getInitialGenesisFile().getAbsolutePath()
                        + "\n> Fork read:  "
                        + cfg.getInitialForkFile().getAbsolutePath()
                        + "\n----------------------------------------------------------------------------\n\n");
    }

    /**
     * Ensures that the { <i>-d</i>, <i>--datadir</i>, <i>-n</i>, <i>--network</i> } arguments work
     * with absolute paths for the database and log. The absolute path overwrites the datadir option
     * location.
     */
    @Test
    @Parameters(method = "parametersWithDirectoryAndNetwork")
    public void testDirectoryAndNetwork_wAbsoluteDbAndLogPath(
            String[] input, ReturnType expectedReturn, String expectedPath) {

        File db = new File(alternativePath, "database");
        cfg.getDb().setPath(db.getAbsolutePath());
        File log = new File(alternativePath, "log");
        cfg.getLog().setLogPath(log.getAbsolutePath());

        // save and reload for changes to take effect
        cfg.toXML(null, cfg.getInitialConfigFile());
        cfg.fromXML();

        assertThat(cli.call(input, cfg)).isEqualTo(expectedReturn);
        assertThat(cfg.getBasePath()).isEqualTo(expectedPath);

        assertThat(cfg.getDatabaseDir()).isNotEqualTo(new File(expectedPath, "database"));
        assertThat(cfg.getDatabaseDir()).isEqualTo(db);

        assertThat(cfg.getLogDir()).isNotEqualTo(new File(expectedPath, "log"));
        assertThat(cfg.getLogDir()).isEqualTo(log);

        if (verbose) {
            printPaths(cfg);
        }
    }

    /** Parameters for testing {@link #testConfig(String[], File, String)}. */
    @SuppressWarnings("unused")
    private Object parametersWithConfig() {
        List<Object> parameters = new ArrayList<>();

        String[] options = new String[] {"-c", "--config"};
        String expected = MAIN_BASE_PATH.getAbsolutePath();

        for (String op : options) {
            // without parameter
            parameters.add(new Object[] {new String[] {op}, mainnetConfig, expected});
            // invalid parameter
            parameters.add(new Object[] {new String[] {op, "invalid"}, mainnetConfig, expected});
            // mainnet as parameter
            parameters.add(new Object[] {new String[] {op, "mainnet"}, mainnetConfig, expected});
        }

        expected = TEST_BASE_PATH.getAbsolutePath();

        for (String op : options) {
            // mastery as parameter
            parameters.add(new Object[] {new String[] {op, "mastery"}, testnetConfig, expected});
            // testnet as parameter
            parameters.add(new Object[] {new String[] {op, "testnet"}, testnetConfig, expected});
        }

        expected = AVM_TEST_BASE_PATH.getAbsolutePath();

        for (String op : options) {
            // avmtestnet as parameter
            parameters.add(
                    new Object[] {new String[] {op, "avmtestnet"}, avmtestnetConfig, expected});
        }

        // config and directory
        String[] dir_options = new String[] {"-d", "--datadir"};
        File config =
                new File(
                        path,
                        "mainnet" + File.separator + "config" + File.separator + configFileName);
        expected = new File(path, "mainnet").getAbsolutePath();

        String[] net_values = new String[] {"mainnet", "invalid"};
        for (String opDir : dir_options) {
            for (String opCfg : options) {
                for (String valNet : net_values) {
                    // with relative path
                    parameters.add(
                            new Object[] {
                                new String[] {opDir, dataDirectory, opCfg, valNet}, config, expected
                            });
                    parameters.add(
                            new Object[] {
                                new String[] {opCfg, valNet, opDir, dataDirectory}, config, expected
                            });
                    // with absolute path
                    parameters.add(
                            new Object[] {
                                new String[] {opDir, path.getAbsolutePath(), opCfg, valNet},
                                config,
                                expected
                            });
                    parameters.add(
                            new Object[] {
                                new String[] {opCfg, valNet, opDir, path.getAbsolutePath()},
                                config,
                                expected
                            });
                }
            }
        }

        // config and directory with testnet
        net_values = new String[] {"mastery", "testnet"};
        config =
                new File(
                        path,
                        "mastery" + File.separator + "config" + File.separator + configFileName);
        expected = new File(path, "mastery").getAbsolutePath();
        for (String opDir : dir_options) {
            for (String opCfg : options) {
                for (String netVal : net_values) {
                    // with relative path
                    parameters.add(
                            new Object[] {
                                new String[] {opDir, dataDirectory, opCfg, netVal}, config, expected
                            });
                    parameters.add(
                            new Object[] {
                                new String[] {opCfg, netVal, opDir, dataDirectory}, config, expected
                            });
                    // with absolute path
                    parameters.add(
                            new Object[] {
                                new String[] {opDir, path.getAbsolutePath(), opCfg, netVal},
                                config,
                                expected
                            });
                    parameters.add(
                            new Object[] {
                                new String[] {opCfg, netVal, opDir, path.getAbsolutePath()},
                                config,
                                expected
                            });
                }
            }
        }

        return parameters.toArray();
    }

    /** Ensures that the { <i>-c</i>, <i>--config</i> } arguments work. */
    @Test
    @Parameters(method = "parametersWithConfig")
    public void testConfig(String[] input, File expectedFile, String expectedPath) {
        if (expectedFile.exists()) {
            assertThat(cfg.fromXML(expectedFile)).isTrue();
        }

        assertThat(cli.call(input, cfg)).isEqualTo(EXIT);
        assertThat(cfg.getBasePath()).isEqualTo(expectedPath);
        assertThat(cfg.getExecConfigFile())
                .isEqualTo(new File(expectedPath, "config" + File.separator + configFileName));
        assertThat(cfg.getExecGenesisFile())
                .isEqualTo(new File(expectedPath, "config" + File.separator + genesisFileName));
        assertThat(cfg.getExecForkFile())
                .isEqualTo(new File(expectedPath, "config" + File.separator + forkFileName));

        assertThat(expectedFile.exists()).isTrue();
        assertThat(cfg.fromXML(expectedFile)).isFalse();

        if (verbose) {
            printPaths(cfg);
        }
    }

    /** Parameters for testing {@link #testConfig_oldLocation(String[], String)}. */
    @SuppressWarnings("unused")
    private Object parametersWithoutMigration() {
        List<Object> parameters = new ArrayList<>();

        String[] options = new String[] {"-c", "--config"};
        String expected = MAIN_BASE_PATH.getAbsolutePath();

        for (String op : options) {
            // invalid parameter
            parameters.add(new Object[] {new String[] {op, "invalid"}, expected});
            // mainnet as parameter
            parameters.add(new Object[] {new String[] {op, "mainnet"}, expected});
        }

        expected = TEST_BASE_PATH.getAbsolutePath();

        for (String op : options) {
            // mastery as parameter
            parameters.add(new Object[] {new String[] {op, "mastery"}, expected});
            // testnet as parameter
            parameters.add(new Object[] {new String[] {op, "testnet"}, expected});
        }

        return parameters.toArray();
    }

    /**
     * Ensures that the { <i>-c</i>, <i>--config</i> } arguments work when using old config
     * location.
     */
    @Test
    @Parameters(method = "parametersWithoutMigration")
    public void testConfig_oldLocation(String[] input, String expectedPath) {
        // ensure config exists on disk at expected location for old kernel
        if (!oldConfig.exists()) {
            File configPath = CONFIG_PATH;
            if (!configPath.exists()) {
                assertThat(configPath.mkdirs()).isTrue();
            }
            cfg.toXML(null, oldConfig);
            Cli.copyRecursively(genesis, oldGenesis);
        }

        assertThat(cli.call(input, cfg)).isEqualTo(EXIT);

        // the config used it for mainnet, therefore will use the MAIN_BASE_PATH
        assertThat(cfg.getBasePath()).isEqualTo(expectedPath);

        assertThat(cfg.getExecConfigFile())
                .isEqualTo(new File(expectedPath, "config" + File.separator + configFileName));
        assertThat(cfg.getExecGenesisFile())
                .isEqualTo(new File(expectedPath, "config" + File.separator + genesisFileName));

        // database, keystore & log are absolute and at old location
        assertThat(cfg.getDatabaseDir()).isEqualTo(new File(expectedPath, "database"));
        assertThat(cfg.getLogDir()).isEqualTo(new File(expectedPath, "log"));
        assertThat(cfg.getKeystoreDir()).isEqualTo(new File(expectedPath, "keystore"));

        if (verbose) {
            printPaths(cfg);
        }
    }

    /**
     * Ensures that the { <i>-c</i>, <i>--config</i> } arguments work when using old config
     * location.
     */
    @Test
    @Parameters({"-c", "--config"})
    public void testConfig_withMigration(String option) {
        // ensure config exists on disk at expected location for old kernel
        if (!oldConfig.exists()) {
            File configPath = CONFIG_PATH;
            if (!configPath.exists()) {
                assertThat(configPath.mkdirs()).isTrue();
            }
            cfg.toXML(null, oldConfig);
            Cli.copyRecursively(genesis, oldGenesis);
        }

        assertThat(cli.call(new String[] {option}, cfg)).isEqualTo(EXIT);

        // the config used it for mainnet, therefore will use the MAIN_BASE_PATH
        assertThat(cfg.getBasePath()).isEqualTo(MAIN_BASE_PATH.getAbsolutePath());

        assertThat(cfg.getInitialConfigFile()).isEqualTo(mainnetConfig);
        assertThat(cfg.getInitialGenesisFile()).isEqualTo(mainnetGenesis);

        assertThat(cfg.getExecConfigFile())
                .isEqualTo(new File(MAIN_BASE_PATH, "config" + File.separator + configFileName));
        assertThat(cfg.getExecGenesisFile())
                .isEqualTo(new File(MAIN_BASE_PATH, "config" + File.separator + genesisFileName));

        // database, keystore & log are absolute and at old location
        assertThat(cfg.getDatabaseDir()).isEqualTo(new File(BASE_PATH, "database"));
        assertThat(cfg.getLogDir()).isEqualTo(new File(BASE_PATH, "log"));
        assertThat(cfg.getKeystoreDir()).isEqualTo(new File(BASE_PATH, "keystore"));

        if (verbose) {
            printPaths(cfg);
        }

        // cleanup: resetting the mainnet config to original
        Cli.copyRecursively(config, mainnetConfig);
    }

    /** Parameters for testing {@link #testPort(String[], ReturnType, String, String)}. */
    @SuppressWarnings("unused")
    private Object parametersWithPort() {
        List<Object> parameters = new ArrayList<>();

        String[] portOptions = new String[] {"-p", "--port"};
        String[] netOptions = new String[] {"-n", "--network"};
        String[] dirOptions = new String[] {"-d", "--datadir"};
        String expectedPath = MAIN_BASE_PATH.getAbsolutePath();
        String expPathOnError = MAIN_BASE_PATH.getAbsolutePath();
        String expPortOnError = Integer.toString(cfg.getNet().getP2p().getPort());

        // port alone
        for (String opPort : portOptions) {
            // without parameter
            parameters.add(
                    new Object[] {new String[] {opPort}, ERROR, expPathOnError, expPortOnError});
            // with two parameters
            parameters.add(
                    new Object[] {
                        new String[] {opPort, TEST_PORT, TEST_PORT},
                        ERROR,
                        expPathOnError,
                        expPortOnError
                    });
            // with invalid parameter
            parameters.add(
                    new Object[] {
                        new String[] {opPort, INVALID_PORT}, RUN, expPathOnError, expPortOnError
                    });
            parameters.add(
                    new Object[] {
                        new String[] {opPort, "-12345"}, RUN, expPathOnError, expPortOnError
                    });
            parameters.add(
                    new Object[] {
                        new String[] {opPort, "invalid"}, RUN, expPathOnError, expPortOnError
                    });
            // with testing port number
            parameters.add(
                    new Object[] {new String[] {opPort, TEST_PORT}, RUN, expectedPath, TEST_PORT});
        }

        // port with help and version
        for (String opPort : portOptions) {
            parameters.add(
                    new Object[] {
                        new String[] {opPort, TEST_PORT, "-h"}, EXIT, expectedPath, expPortOnError
                    });
            parameters.add(
                    new Object[] {
                        new String[] {opPort, TEST_PORT, "-v"}, EXIT, expectedPath, expPortOnError
                    });
        }

        // network and port
        String[] netValues = new String[] {"mainnet", "invalid"};
        for (String opNet : netOptions) {
            for (String valNet : netValues) {
                for (String opPort : portOptions) {
                    // without port parameter
                    parameters.add(
                            new Object[] {
                                new String[] {opNet, valNet, opPort},
                                ERROR,
                                expPathOnError,
                                expPortOnError
                            });
                    parameters.add(
                            new Object[] {
                                new String[] {opPort, opNet, valNet},
                                ERROR,
                                expPathOnError,
                                expPortOnError
                            });
                    // with invalid port parameter
                    parameters.add(
                            new Object[] {
                                new String[] {opNet, valNet, opPort, INVALID_PORT},
                                RUN,
                                expPathOnError,
                                expPortOnError
                            });
                    parameters.add(
                            new Object[] {
                                new String[] {opPort, INVALID_PORT, opNet, valNet},
                                RUN,
                                expPathOnError,
                                expPortOnError
                            });
                    // with testing port number
                    parameters.add(
                            new Object[] {
                                new String[] {opNet, valNet, opPort, TEST_PORT},
                                RUN,
                                expectedPath,
                                TEST_PORT
                            });
                    parameters.add(
                            new Object[] {
                                new String[] {opPort, TEST_PORT, opNet, valNet},
                                RUN,
                                expectedPath,
                                TEST_PORT
                            });
                }
            }
        }

        // network and port with testnet
        netValues = new String[] {"mastery", "testnet"};
        expectedPath = TEST_BASE_PATH.getAbsolutePath();
        for (String opNet : netOptions) {
            for (String valNet : netValues) {
                for (String opPort : portOptions) {
                    parameters.add(
                            new Object[] {
                                new String[] {opNet, valNet, opPort, TEST_PORT},
                                RUN,
                                expectedPath,
                                TEST_PORT
                            });
                    parameters.add(
                            new Object[] {
                                new String[] {opPort, TEST_PORT, opNet, valNet},
                                RUN,
                                expectedPath,
                                TEST_PORT
                            });
                }
            }
        }

        // directory and port
        String[] dirValues = new String[] {dataDirectory, path.getAbsolutePath()};
        expectedPath = new File(path, "mainnet").getAbsolutePath();
        for (String opDir : dirOptions) {
            for (String valDir : dirValues) {
                for (String opPort : portOptions) {
                    // without port parameter
                    parameters.add(
                            new Object[] {
                                new String[] {opDir, valDir, opPort},
                                ERROR,
                                expPathOnError,
                                expPortOnError
                            });
                    parameters.add(
                            new Object[] {
                                new String[] {opPort, opDir, valDir},
                                ERROR,
                                expPathOnError,
                                expPortOnError
                            });
                    // with invalid port parameter
                    parameters.add(
                            new Object[] {
                                new String[] {opDir, valDir, opPort, INVALID_PORT},
                                RUN,
                                expectedPath,
                                expPortOnError
                            });
                    parameters.add(
                            new Object[] {
                                new String[] {opPort, INVALID_PORT, opDir, valDir},
                                RUN,
                                expectedPath,
                                expPortOnError
                            });
                    // with testing port number
                    parameters.add(
                            new Object[] {
                                new String[] {opDir, valDir, opPort, TEST_PORT},
                                RUN,
                                expectedPath,
                                TEST_PORT
                            });
                    parameters.add(
                            new Object[] {
                                new String[] {opPort, TEST_PORT, opDir, valDir},
                                RUN,
                                expectedPath,
                                TEST_PORT
                            });
                }
            }
        }

        // network, directory and port
        netValues = new String[] {"mainnet", "mastery"};
        for (String opNet : netOptions) {
            for (String valNet : netValues) {
                for (String opDir : dirOptions) {
                    for (String valDir : dirValues) {
                        for (String opPort : portOptions) {
                            expectedPath = new File(path, valNet).getAbsolutePath();
                            parameters.add(
                                    new Object[] {
                                        new String[] {
                                            opNet, valNet, opDir, valDir, opPort, TEST_PORT
                                        },
                                        RUN,
                                        expectedPath,
                                        TEST_PORT
                                    });
                        }
                    }
                }
            }
        }

        // directory with subdirectories and port
        String dir = dataDirectory + File.separator + "subfolder";
        File testPath = new File(BASE_PATH, dir);
        expectedPath = new File(testPath, "mainnet").getAbsolutePath();
        for (String opDir : dirOptions) {
            for (String opPort : portOptions) {
                // with relative path with subdirectories
                parameters.add(
                        new Object[] {
                            new String[] {opDir, dir, opPort, TEST_PORT},
                            RUN,
                            expectedPath,
                            TEST_PORT
                        });
            }
        }

        // port with config and directory
        expectedPath = new File(path, "mainnet").getAbsolutePath();
        for (String opPort : portOptions) {
            // with relative path
            parameters.add(
                    new Object[] {
                        new String[] {
                            "--datadir", dataDirectory, "--config", "mainnet", opPort, TEST_PORT
                        },
                        EXIT,
                        expectedPath,
                        TEST_PORT
                    });
            parameters.add(
                    new Object[] {
                        new String[] {
                            "-c", "mainnet", opPort, TEST_PORT, "--datadir", dataDirectory
                        },
                        EXIT,
                        expectedPath,
                        TEST_PORT
                    });
            // with absolute path
            parameters.add(
                    new Object[] {
                        new String[] {
                            opPort, TEST_PORT, "-d", path.getAbsolutePath(), "--config", "mainnet"
                        },
                        EXIT,
                        expectedPath,
                        TEST_PORT
                    });
            parameters.add(
                    new Object[] {
                        new String[] {
                            "-c", "mainnet", "-d", path.getAbsolutePath(), opPort, TEST_PORT
                        },
                        EXIT,
                        expectedPath,
                        TEST_PORT
                    });
        }

        return parameters.toArray();
    }

    @Test
    @Parameters(method = "parametersWithPort")
    public void testPort(
            String[] input, ReturnType expectedReturn, String expectedPath, String expectedPort) {

        cfg.toXML(new String[] {"--p2p=" + "," + DEFAULT_PORT}, cfg.getInitialConfigFile());

        assertThat(cli.call(input, cfg)).isEqualTo(expectedReturn);
        assertThat(cfg.getBasePath()).isEqualTo(expectedPath);
        assertThat(cfg.getExecConfigFile())
                .isEqualTo(new File(expectedPath, "config" + File.separator + configFileName));
        assertThat(cfg.getExecGenesisFile())
                .isEqualTo(new File(expectedPath, "config" + File.separator + genesisFileName));
        assertThat(cfg.getExecForkFile())
                .isEqualTo(new File(expectedPath, "config" + File.separator + forkFileName));
        assertThat(cfg.getDatabaseDir()).isEqualTo(new File(expectedPath, "database"));
        assertThat(cfg.getLogDir()).isEqualTo(new File(expectedPath, "log"));
        assertThat(cfg.getKeystoreDir()).isEqualTo(new File(expectedPath, "keystore"));
        // test port is updated
        assertThat(Integer.toString(cfg.getNet().getP2p().getPort())).isEqualTo(expectedPort);
        // test port in initial config is unchanged
        cfg.resetInternal();
        cfg.fromXML();
        assertThat(Integer.toString(cfg.getNet().getP2p().getPort())).isEqualTo(DEFAULT_PORT);

        if (verbose) {
            printPaths(cfg);
        }
    }

    /**
     * Parameters for testing {@link #testForceCompact(String[], ReturnType, String, boolean, int,
     * int)}.
     */
    @SuppressWarnings("unused")
    private Object parametersWithForceCompact() {
        List<Object> parameters = new ArrayList<>();

        String expectedPath = MAIN_BASE_PATH.getAbsolutePath();
        String expPathOnError = MAIN_BASE_PATH.getAbsolutePath();
        String opCompact = "--force-compact";

        // Compact alone
        // without parameter
        parameters.add(
                new Object[] {
                    new String[] {opCompact},
                    ERROR,
                    expPathOnError,
                    false,
                    SLOW_IMPORT_TIME,
                    COMPACT_FREQUENCY
                });
        // with one parameter
        parameters.add(
                new Object[] {
                    new String[] {opCompact, "123"},
                    RUN,
                    expPathOnError,
                    false,
                    SLOW_IMPORT_TIME,
                    COMPACT_FREQUENCY
                });
        parameters.add(
                new Object[] {
                    new String[] {opCompact, "invalid"},
                    RUN,
                    expPathOnError,
                    false,
                    SLOW_IMPORT_TIME,
                    COMPACT_FREQUENCY
                });
        parameters.add(
                new Object[] {
                    new String[] {opCompact, "TRUE"},
                    RUN,
                    expectedPath,
                    true,
                    SLOW_IMPORT_TIME,
                    COMPACT_FREQUENCY
                });
        parameters.add(
                new Object[] {
                    new String[] {opCompact, "false"},
                    RUN,
                    expectedPath,
                    false,
                    SLOW_IMPORT_TIME,
                    COMPACT_FREQUENCY
                });
        // with two parameters
        parameters.add(
                new Object[] {
                    new String[] {opCompact, "b", "c"},
                    RUN,
                    expectedPath,
                    false,
                    SLOW_IMPORT_TIME,
                    COMPACT_FREQUENCY
                });
        parameters.add(
                new Object[] {
                    new String[] {opCompact, "-1000", "3.14"},
                    RUN,
                    expectedPath,
                    false,
                    SLOW_IMPORT_TIME,
                    COMPACT_FREQUENCY
                });
        parameters.add(
                new Object[] {
                    new String[] {opCompact, "123", "456"}, RUN, expectedPath, true, 123, 456
                });
        // with more than two parameters
        parameters.add(
                new Object[] {
                    new String[] {opCompact, "true", "123", "456"},
                    ERROR,
                    expectedPath,
                    false,
                    SLOW_IMPORT_TIME,
                    COMPACT_FREQUENCY
                });

        // compact with help and version
        parameters.add(
                new Object[] {
                    new String[] {opCompact, "true", "-h"},
                    EXIT,
                    expectedPath,
                    false,
                    SLOW_IMPORT_TIME,
                    COMPACT_FREQUENCY
                });
        parameters.add(
                new Object[] {
                    new String[] {opCompact, "true", "-v"},
                    EXIT,
                    expectedPath,
                    false,
                    SLOW_IMPORT_TIME,
                    COMPACT_FREQUENCY
                });

        // compact with network
        String[] netValues = new String[] {"mainnet", "invalid"};
        for (String valNet : netValues) {
            // without compact parameter
            parameters.add(
                    new Object[] {
                        new String[] {opCompact, "-n", valNet},
                        ERROR,
                        expectedPath,
                        false,
                        SLOW_IMPORT_TIME,
                        COMPACT_FREQUENCY
                    });
            // with invalid compact parameter
            parameters.add(
                    new Object[] {
                        new String[] {"-n", valNet, opCompact, "-123", "456"},
                        RUN,
                        expectedPath,
                        false,
                        SLOW_IMPORT_TIME,
                        COMPACT_FREQUENCY
                    });
            parameters.add(
                    new Object[] {
                        new String[] {"-n", valNet, opCompact, "invalid", "123"},
                        RUN,
                        expectedPath,
                        false,
                        SLOW_IMPORT_TIME,
                        COMPACT_FREQUENCY
                    });
            // with valid compact parameter
            parameters.add(
                    new Object[] {
                        new String[] {
                            opCompact, "true", "-n", valNet,
                        },
                        RUN,
                        expectedPath,
                        true,
                        SLOW_IMPORT_TIME,
                        COMPACT_FREQUENCY
                    });
            parameters.add(
                    new Object[] {
                        new String[] {"-n", valNet, opCompact, "123", "456"},
                        RUN,
                        expectedPath,
                        true,
                        123,
                        456
                    });
        }
        // compact with network testnet
        netValues = new String[] {"mastery", "testnet"};
        expectedPath = TEST_BASE_PATH.getAbsolutePath();
        for (String valNet : netValues) {
            parameters.add(
                    new Object[] {
                        new String[] {
                            opCompact, "true", "-n", valNet,
                        },
                        RUN,
                        expectedPath,
                        true,
                        SLOW_IMPORT_TIME,
                        COMPACT_FREQUENCY
                    });
            parameters.add(
                    new Object[] {
                        new String[] {"-n", valNet, opCompact, "123", "456"},
                        RUN,
                        expectedPath,
                        true,
                        123,
                        456
                    });
        }
        // compact and directory
        String[] dirValues = new String[] {dataDirectory, path.getAbsolutePath()};
        expectedPath = new File(path, "mainnet").getAbsolutePath();
        for (String valDir : dirValues) {
            // without compact parameter
            parameters.add(
                    new Object[] {
                        new String[] {opCompact, "-d", valDir},
                        ERROR,
                        expPathOnError,
                        false,
                        SLOW_IMPORT_TIME,
                        COMPACT_FREQUENCY
                    });
            // with invalid compact parameter
            parameters.add(
                    new Object[] {
                        new String[] {opCompact, "invliad", "-d", valDir},
                        RUN,
                        expectedPath,
                        false,
                        SLOW_IMPORT_TIME,
                        COMPACT_FREQUENCY
                    });
            parameters.add(
                    new Object[] {
                        new String[] {opCompact, "123", "-d", valDir},
                        RUN,
                        expectedPath,
                        false,
                        SLOW_IMPORT_TIME,
                        COMPACT_FREQUENCY
                    });
            parameters.add(
                    new Object[] {
                        new String[] {opCompact, "-123", "1.234", "-d", valDir},
                        RUN,
                        expectedPath,
                        false,
                        SLOW_IMPORT_TIME,
                        COMPACT_FREQUENCY
                    });
            // with valid compact parameter
            parameters.add(
                    new Object[] {
                        new String[] {opCompact, "true", "-d", valDir},
                        RUN,
                        expectedPath,
                        true,
                        SLOW_IMPORT_TIME,
                        COMPACT_FREQUENCY
                    });
            parameters.add(
                    new Object[] {
                        new String[] {opCompact, "123", "456", "-d", valDir},
                        RUN,
                        expectedPath,
                        true,
                        123,
                        456
                    });
        }

        // compact and port
        expectedPath = MAIN_BASE_PATH.getAbsolutePath();
        parameters.add(
                new Object[] {
                    new String[] {opCompact, "invalid", "-p", TEST_PORT},
                    RUN,
                    expectedPath,
                    false,
                    SLOW_IMPORT_TIME,
                    COMPACT_FREQUENCY
                });
        parameters.add(
                new Object[] {
                    new String[] {opCompact, "-123", "456", "-p", TEST_PORT},
                    RUN,
                    expectedPath,
                    false,
                    SLOW_IMPORT_TIME,
                    COMPACT_FREQUENCY
                });
        parameters.add(
                new Object[] {
                    new String[] {opCompact, "true", "-p", TEST_PORT},
                    RUN,
                    expectedPath,
                    true,
                    SLOW_IMPORT_TIME,
                    COMPACT_FREQUENCY
                });
        parameters.add(
                new Object[] {
                    new String[] {opCompact, "123", "456", "-p", TEST_PORT},
                    RUN,
                    expectedPath,
                    true,
                    123,
                    456
                });

        // compact with network, directory and port
        netValues = new String[] {"mainnet", "mastery"};
        for (String valNet : netValues) {
            for (String valDir : dirValues) {
                expectedPath = new File(path, valNet).getAbsolutePath();
                parameters.add(
                        new Object[] {
                            new String[] {
                                "-n", valNet, "-d", valDir, "-p", TEST_PORT, opCompact, "true"
                            },
                            RUN,
                            expectedPath,
                            true,
                            SLOW_IMPORT_TIME,
                            COMPACT_FREQUENCY
                        });
                parameters.add(
                        new Object[] {
                            new String[] {
                                "-n", valNet, "-d", valDir, "-p", TEST_PORT, opCompact, "123", "456"
                            },
                            RUN,
                            expectedPath,
                            true,
                            123,
                            456
                        });
            }
        }

        // compact with config and directory
        expectedPath = new File(path, "mainnet").getAbsolutePath();
        // with relative path
        parameters.add(
                new Object[] {
                    new String[] {
                        "-d", dataDirectory, "-c", "mainnet", "-p", TEST_PORT, opCompact, "true"
                    },
                    EXIT,
                    expectedPath,
                    true,
                    SLOW_IMPORT_TIME,
                    COMPACT_FREQUENCY
                });
        // with absolute path
        parameters.add(
                new Object[] {
                    new String[] {
                        "-d",
                        path.getAbsolutePath(),
                        "-c",
                        "mainnet",
                        "-p",
                        TEST_PORT,
                        opCompact,
                        "123",
                        "456"
                    },
                    EXIT,
                    expectedPath,
                    true,
                    123,
                    456
                });

        return parameters.toArray();
    }

    @Test
    @Parameters(method = "parametersWithForceCompact")
    public void testForceCompact(
            String[] input,
            ReturnType expectedReturn,
            String expectedPath,
            boolean expectedCompactEnabled,
            int expectedSlowImportTime,
            int expectedCompactFrequency) {

        assertThat(cli.call(input, cfg)).isEqualTo(expectedReturn);
        assertThat(cfg.getBasePath()).isEqualTo(expectedPath);
        assertThat(cfg.getExecConfigFile())
                .isEqualTo(new File(expectedPath, "config" + File.separator + configFileName));
        assertThat(cfg.getExecGenesisFile())
                .isEqualTo(new File(expectedPath, "config" + File.separator + genesisFileName));
        assertThat(cfg.getExecForkFile())
                .isEqualTo(new File(expectedPath, "config" + File.separator + forkFileName));
        assertThat(cfg.getDatabaseDir()).isEqualTo(new File(expectedPath, "database"));
        // check compact configurations in exec config are updated
        assertThat(cfg.getSync().getCompactEnabled()).isEqualTo(expectedCompactEnabled);
        assertThat(cfg.getSync().getSlowImportTime()).isEqualTo(expectedSlowImportTime);
        assertThat(cfg.getSync().getCompactFrequency()).isEqualTo(expectedCompactFrequency);
        // check compact configurations in initial config are unchanged
        cfg.resetInternal();
        cfg.fromXML();
        assertThat(cfg.getSync().getCompactEnabled()).isEqualTo(false);
        assertThat(cfg.getSync().getSlowImportTime()).isEqualTo(SLOW_IMPORT_TIME);
        assertThat(cfg.getSync().getCompactFrequency()).isEqualTo(COMPACT_FREQUENCY);
    }

    /** Parameters for testing {@link #testInfo(String[], ReturnType, String)}. */
    @SuppressWarnings("unused")
    private Object parametersWithInfo() {
        List<Object> parameters = new ArrayList<>();

        String[] options = new String[] {"-i", "--info"};
        String expected = MAIN_BASE_PATH.getAbsolutePath();
        String expOnError = expected;

        // only info
        for (String op : options) {
            // without parameter
            parameters.add(new Object[] {new String[] {op}, EXIT, expected});
            // invalid parameter
            parameters.add(new Object[] {new String[] {op, "value"}, ERROR, expOnError});
        }

        // with network
        expected = TEST_BASE_PATH.getAbsolutePath();
        for (String op : options) {
            // mastery as parameter
            parameters.add(new Object[] {new String[] {op, "-n", "mastery"}, EXIT, expected});
            parameters.add(new Object[] {new String[] {"-n", "mastery", op}, EXIT, expected});
            // invalid parameter
            parameters.add(
                    new Object[] {new String[] {op, "value", "-n", "mastery"}, ERROR, expOnError});
        }

        // with directory
        expected = new File(path, "mainnet").getAbsolutePath();
        for (String op : options) {
            // with relative path
            parameters.add(new Object[] {new String[] {op, "-d", dataDirectory}, EXIT, expected});
            parameters.add(new Object[] {new String[] {"-d", dataDirectory, op}, EXIT, expected});
            // + invalid parameter
            parameters.add(
                    new Object[] {
                        new String[] {op, "value", "-d", dataDirectory}, ERROR, expOnError
                    });
            // with absolute path
            parameters.add(
                    new Object[] {new String[] {op, "-d", path.getAbsolutePath()}, EXIT, expected});
            parameters.add(
                    new Object[] {new String[] {"-d", path.getAbsolutePath(), op}, EXIT, expected});
            // + invalid parameter
            parameters.add(
                    new Object[] {
                        new String[] {op, "value", "-d", path.getAbsolutePath()}, ERROR, expOnError
                    });
        }

        // with port
        expected = MAIN_BASE_PATH.getAbsolutePath();
        for (String op : options) {
            // test port number as parameter
            parameters.add(new Object[] {new String[] {op, "-p", TEST_PORT}, EXIT, expected});
            parameters.add(new Object[] {new String[] {"-p", TEST_PORT, op}, EXIT, expected});
            // invalid port parameter
            parameters.add(new Object[] {new String[] {op, "-p", INVALID_PORT}, EXIT, expOnError});
        }

        // with compact
        for (String op : options) {
            // test port number as parameter
            parameters.add(
                    new Object[] {
                        new String[] {op, "--force-compact", "invalid"}, EXIT, expOnError
                    });
            parameters.add(
                    new Object[] {new String[] {op, "--force-compact", "true"}, EXIT, expected});
            parameters.add(
                    new Object[] {
                        new String[] {op, "--force-compact", "123", "456"}, EXIT, expected
                    });
        }

        // with port and directory
        expected = new File(path, "mainnet").getAbsolutePath();
        for (String op : options) {
            // with relative path
            parameters.add(
                    new Object[] {
                        new String[] {op, "-d", dataDirectory, "-p", TEST_PORT}, EXIT, expected
                    });
            // with absolute path
            parameters.add(
                    new Object[] {
                        new String[] {op, "-p", TEST_PORT, "-d", path.getAbsolutePath()},
                        EXIT,
                        expected
                    });
        }

        // with network and directory
        expected = new File(path, "mastery").getAbsolutePath();

        for (String op : options) {
            // with relative path
            parameters.add(
                    new Object[] {
                        new String[] {op, "-d", dataDirectory, "-n", "mastery"}, EXIT, expected
                    });
            parameters.add(
                    new Object[] {
                        new String[] {"-n", "mastery", op, "-d", dataDirectory}, EXIT, expected
                    });
            parameters.add(
                    new Object[] {
                        new String[] {"-n", "mastery", "-d", dataDirectory, op}, EXIT, expected
                    });
            // with absolute path
            parameters.add(
                    new Object[] {
                        new String[] {op, "-n", "mastery", "-d", path.getAbsolutePath()},
                        EXIT,
                        expected
                    });
            parameters.add(
                    new Object[] {
                        new String[] {"-d", path.getAbsolutePath(), op, "-n", "mastery"},
                        EXIT,
                        expected
                    });
            parameters.add(
                    new Object[] {
                        new String[] {"-d", path.getAbsolutePath(), "-n", "mastery", op},
                        EXIT,
                        expected
                    });
        }

        return parameters.toArray();
    }

    /** Ensures that the { <i>-i</i>, <i>--info</i> } arguments work. */
    @Test
    @Parameters(method = "parametersWithInfo")
    public void testInfo(String[] input, ReturnType expectedReturn, String expectedPath) {
        assertThat(cli.call(input, cfg)).isEqualTo(expectedReturn);
        assertThat(cfg.getBasePath()).isEqualTo(expectedPath);
    }

    /**
     * Ensures that the { <i>-i</i>, <i>--info</i> } arguments work when using old config location.
     */
    @Test
    @Parameters({"-i", "--info"})
    public void testInfoWithMigration(String option) {
        // ensure config exists on disk at expected location for old kernel
        if (!oldConfig.exists()) {
            File configPath = CONFIG_PATH;
            if (!configPath.exists()) {
                assertThat(configPath.mkdirs()).isTrue();
            }
            cfg.toXML(null, oldConfig);
            Cli.copyRecursively(genesis, oldGenesis);
        }

        assertThat(cli.call(new String[] {option}, cfg)).isEqualTo(EXIT);
        assertThat(cfg.getBasePath()).isEqualTo(MAIN_BASE_PATH.getAbsolutePath());

        // database, keystore & log are absolute and at old location
        assertThat(cfg.getDatabaseDir()).isEqualTo(new File(BASE_PATH, "database"));
        assertThat(cfg.getLogDir()).isEqualTo(new File(BASE_PATH, "log"));
        assertThat(cfg.getKeystoreDir()).isEqualTo(new File(BASE_PATH, "keystore"));

        // cleanup: resetting the mainnet config to original
        Cli.copyRecursively(config, mainnetConfig);
    }

    /**
     * Ensures that the { <i>-i</i>, <i>--info</i> } arguments work when using the config location
     * copied to the execution directory.
     */
    @Test
    @Parameters({"-i", "--info"})
    public void testInfo_execLocation(String option) {
        // generates the config at the required destination
        cli.call(new String[] {"-c", "-d", dataDirectory}, cfg);

        // ensure config exists on disk at expected location for old kernel
        assertThat(cfg.getExecConfigFile().exists()).isTrue();

        assertThat(cli.call(new String[] {option, "-d", dataDirectory}, cfg)).isEqualTo(EXIT);
    }

    private List<Object> parametersWithAccount(String[] options) {
        List<Object> parameters = new ArrayList<>();

        // only info
        for (String op : options) {
            String[] params = op.split("\\s");
            String param1 =params[0];
            String param2 =params[1];

                // without parameter
            parameters.add(new Object[] {params, EXIT});
            // invalid parameter
            parameters.add(new Object[] {new String[] {param1, param2, "value"}, ERROR});
        }

        // with network
        for (String op : options) {
            String[] params = op.split("\\s");
            String param1 =params[0];
            String param2 =params[1];
            // mastery as parameter
            parameters.add(new Object[] {new String[] {param1, param2, "-n", "mastery"}, ERROR});
            parameters.add(new Object[] {new String[] {"-n", "mastery", param1, param2}, EXIT});
            // invalid parameter
            parameters.add(new Object[] {new String[] {param1, param2, "value", "-n", "mastery"}, ERROR});
        }

        // with directory
        for (String op : options) {
            String[] params = op.split("\\s");
            String param1 =params[0];
            String param2 =params[1];
            // with relative path
            parameters.add(new Object[] {new String[] {param1, param2, "-d", dataDirectory}, ERROR});
            parameters.add(new Object[] {new String[] {"-d", dataDirectory, param1, param2}, EXIT});
            // + invalid parameter
            parameters.add(new Object[] {new String[] {param1, param2, "value", "-d", dataDirectory}, ERROR});
            // with absolute path
            parameters.add(new Object[] {new String[] {param1, param2, "-d", path.getAbsolutePath()}, ERROR});
            parameters.add(new Object[] {new String[] {"-d", path.getAbsolutePath(), param1, param2}, EXIT});
            // + invalid parameter
            parameters.add(
                    new Object[] {new String[] {param1, param2, "value", "-d", path.getAbsolutePath()}, ERROR});
        }

        // with network and directory
        for (String op : options) {
            String[] params = op.split("\\s");
            String param1 =params[0];
            String param2 =params[1];
            // with relative path
            parameters.add(
                    new Object[] {new String[] {param1, param2, "-d", dataDirectory, "-n", "mastery"}, ERROR});
            parameters.add(
                    new Object[] {new String[] {"-n", "mastery", param1, param2, "-d", dataDirectory}, ERROR});
            parameters.add(
                    new Object[] {new String[] {"-n", "mastery", "-d", dataDirectory, param1, param2}, EXIT});
            // with absolute path
            parameters.add(
                    new Object[] {
                        new String[] {param1, param2, "-n", "mastery", "-d", path.getAbsolutePath()}, ERROR
                    });
            parameters.add(
                    new Object[] {
                        new String[] {"-d", path.getAbsolutePath(), param1, param2, "-n", "mastery"}, ERROR
                    });
            parameters.add(
                    new Object[] {
                        new String[] {"-d", path.getAbsolutePath(), "-n", "mastery", param1, param2}, EXIT
                    });
        }

        return parameters;
    }

    /** Parameters for testing {@link #testCreateAccount(String[], ReturnType)}. */
    @SuppressWarnings("unused")
    private Object parametersWithCreateAccount() {
        return parametersWithAccount(new String[] {"a create", "a c", "account create","account c"})
                .toArray();
    }

    /** Ensures that the { <i>-a create</i>, <i>ac</i> } arguments work. */
    @Test
    @Parameters(method = "parametersWithCreateAccount")
    public void testCreateAccount(String[] input, ReturnType expectedReturn) {
        // number of accounts before create call
        int count = Keystore.list().length;
        assertThat(mockCli.call(input, cfg)).isEqualTo(expectedReturn);
        if (expectedReturn == EXIT) {
            // ensure number of accounts was incremented
            assertThat(Keystore.list().length).isEqualTo(count + 1);
        } else {
            // ensure number of accounts is unchanged
            assertThat(Keystore.list().length).isEqualTo(count);
        }
    }


    // TODO: add test for create account with old config location

    /** Parameters for testing {@link #testListAccounts(String[], ReturnType)}. */
    @SuppressWarnings("unused")
    private Object parametersWithListAccount() {
        return parametersWithAccount(new String[] {"a list", "a l", "account l","account list"}).toArray();
    }

    /** Ensures that the { <i>-a list</i>, <i>al</i> } arguments work. */
    @Test
    @Parameters(method = "parametersWithListAccount")
    public void testListAccounts(String[] input, ReturnType expectedReturn) {
        assertThat(cli.call(input, cfg)).isEqualTo(expectedReturn);
    }

    /** Ensures that the { <i>-a export</i>, <i>ae</i>, <i>--account export</i> } arguments work. */
    @Test
    @Parameters({"a|export", "a|e", "account|export", "account|e"})
    public void testExportPrivateKey(String command, String option) {
        // create account
        assertThat(mockCli.call(new String[] {"a","c"}, cfg)).isEqualTo(EXIT);

        String account = Keystore.list()[0];
        assertEquals(EXIT, mockCli.call(new String[] {command, option, account}, cfg));
    }

    /**
     * Ensures that the { <i>-a export</i>, <i>ae</i>, <i>--account export</i> } arguments work in
     * combination with the data directory option.
     */
    @Test
    @Parameters({"a|export", "a|e", "account|export","account|export"})
    public void testExportPrivateKey_withDataDir(String command, String option) {
        // create account
        assertThat(mockCli.call(new String[] {"-d", dataDirectory, "a","c"}, cfg)).isEqualTo(EXIT);

        String account = Keystore.list()[0];
        assertEquals(ERROR, mockCli.call(new String[] {command, option, "-d", dataDirectory}, cfg));
        assertEquals(EXIT, mockCli.call(new String[] {"-d", dataDirectory, command, option, account}, cfg));
    }

    /**
     * Ensures that the { <i>-a export</i>, <i>ae</i>, <i>--account export</i> } arguments work in
     * combination with the network option.
     */
    @Test
    @Parameters({"a|export", "a|e", "account|export","account|e"})
    public void testExportPrivateKey_withNetwork(String command, String option) {
        // create account
        assertThat(mockCli.call(new String[] {"-n", "mastery","a","c"}, cfg)).isEqualTo(EXIT);

        String account = Keystore.list()[0];
        assertEquals(ERROR, mockCli.call(new String[] {command, option, account, "-n", "mastery"}, cfg));
        assertEquals(EXIT, mockCli.call(new String[] {"-n", "mastery", command, option, account}, cfg));
    }

    /**
     * Ensures that the { <i>-a export</i>, <i>ae</i>, <i>--account export</i> } arguments work in
     * combination with the data directory and network options.
     */
    @Test
    @Parameters({"a|export", "a|e", "account|export", "account|e"})
    public void testExportPrivateKey_withDataDirAndNetwork(String command, String option) {
        // create account
        assertThat(mockCli.call(new String[] {"-d", dataDirectory, "-n", "mastery" ,"a","c" }, cfg))
                .isEqualTo(EXIT);

        String account = Keystore.list()[0];
        assertEquals(
                ERROR,
                mockCli.call(
                        new String[] {"-n", "mastery", command, option, account, "-d", dataDirectory}, cfg));
        assertEquals(
                EXIT,
                mockCli.call(
                        new String[] {"-n", "mastery", "-d", dataDirectory, command, option, account}, cfg));
        assertEquals(
                ERROR,
                mockCli.call(
                        new String[] {command, option, account, "-d", dataDirectory, "-n", "mastery"}, cfg));
    }

    /**
     * Ensured that the { <i>-a export</i>, <i>ae</i>, <i>--account export</i> } arguments fail when
     * the given an incorrect account value (proper substring of a valid account).
     */
    @Test
    @Parameters({"a|export", "a|e", "account|export","account|e"})
    public void testExportPrivateKey_wSubstringOfAccount(String command, String option) {
        // create account
        assertThat(mockCli.call(new String[] {"a","c"}, cfg)).isEqualTo(EXIT);

        String subStrAcc = Keystore.list()[0].substring(1);

        assertEquals(ERROR, cli.call(new String[] {command, option, subStrAcc}, cfg));
    }

    /** Ensures that the { <i>-a import</i>, <i>a</i>, <i>--account import</i> } arguments work. */
    @Test
    @Parameters({"a|import", "a|i", "account|import","account|i"})
    public void testImportPrivateKey(String command ,String option) {
        // test 1: successful call
        ECKey key = ECKeyFac.inst().create();
        String pKey = Hex.toHexString(key.getPrivKeyBytes());

        assertEquals(EXIT, mockCli.call(new String[] {command, option, pKey}, cfg));
        assertThat(Keystore.list().length).isEqualTo(1);

        // test 2: error -> known key
        assertEquals(ERROR, mockCli.call(new String[] {command, option, pKey}, cfg));
        assertThat(Keystore.list().length).isEqualTo(1);
    }

    /**
     * Ensures that the { <i>-a import</i>, <i>a</i>, <i>--account import</i> } arguments work in
     * combination with the data directory option.
     */
    @Test
    @Parameters({"a|import", "a|i", "account|import","account|i"})
    public void testImportPrivateKey_withDataDir(String command, String option) {
        // test 1: -d last
        ECKey key = ECKeyFac.inst().create();
        String pKey = Hex.toHexString(key.getPrivKeyBytes());

        assertEquals(ERROR, mockCli.call(new String[] {command, option, pKey, "-d", dataDirectory}, cfg));
        assertThat(Keystore.list().length).isEqualTo(0);

        // test 2: -d first
        key = ECKeyFac.inst().create();
        pKey = Hex.toHexString(key.getPrivKeyBytes());

        assertEquals(EXIT, mockCli.call(new String[] {"-d", dataDirectory, command, option, pKey}, cfg));
        assertThat(Keystore.list().length).isEqualTo(1);

        // test 3: known key
        assertEquals(ERROR, mockCli.call(new String[] {"-d", dataDirectory, command, option, pKey}, cfg));
        assertThat(Keystore.list().length).isEqualTo(1);
    }

    /**
     * Ensures that the { <i>-a import</i>, <i>a</i>, <i>--account import</i> } arguments work in
     * combination with the network option.
     */
    @Test
    @Parameters({"a|import", "a|i", "account|import","account|i"})
    public void testImportPrivateKey_withNetwork(String command, String option) {
        // test 1: -n last
        ECKey key = ECKeyFac.inst().create();
        String pKey = Hex.toHexString(key.getPrivKeyBytes());

        assertEquals(ERROR, mockCli.call(new String[] {command, option, pKey, "-n", "mastery"}, cfg));
        assertThat(Keystore.list().length).isEqualTo(0);

        // test 2: -n first
        key = ECKeyFac.inst().create();
        pKey = Hex.toHexString(key.getPrivKeyBytes());

        assertEquals(EXIT, mockCli.call(new String[] {"-n", "mastery", command, option, pKey}, cfg));
        assertThat(Keystore.list().length).isEqualTo(1);
        // test 3: known key
        assertEquals(ERROR, mockCli.call(new String[] { "-n", "mastery", command, option, pKey}, cfg));
        assertThat(Keystore.list().length).isEqualTo(1);
    }

    /**
     * Ensures that the { <i>-a import</i>, <i>a</i>, <i>--account import</i> } arguments work in
     * combination with the data directory and network options.
     */
    @Test
    @Parameters({"a|import", "a|i", "account|import", "account|i"})
    public void testImportPrivateKey_withDataDirAndNetwork(String command, String option) {
        // test 1: -d -n last
        ECKey key = ECKeyFac.inst().create();
        String pKey = Hex.toHexString(key.getPrivKeyBytes());

        assertEquals(
                ERROR,
                mockCli.call(
                        new String[] {command, option, pKey, "-d", dataDirectory, "-n", "mastery"}, cfg));
        assertThat(Keystore.list().length).isEqualTo(0);


        // test 2: -d -n first
        key = ECKeyFac.inst().create();
        pKey = Hex.toHexString(key.getPrivKeyBytes());

        assertEquals(
            EXIT,
            mockCli.call(
                new String[] {"-n", "mastery", "-d", dataDirectory, command, option, pKey}, cfg));
        assertThat(Keystore.list().length).isEqualTo(1);

        // test 3: duplicate key
        assertEquals(
            ERROR,
            mockCli.call(
                new String[] {"-n", "mastery", "-d", dataDirectory, command, option, pKey}, cfg));
        assertThat(Keystore.list().length).isEqualTo(1);

        // test 4: ai middle
        key = ECKeyFac.inst().create();
        pKey = Hex.toHexString(key.getPrivKeyBytes());

        assertEquals(
                ERROR,
                mockCli.call(
                        new String[] {"-n", "mastery", command, option, pKey, "-d", dataDirectory}, cfg));
        assertThat(Keystore.list().length).isEqualTo(1);
    }

    /**
     * Ensures that the { <i>-a import</i>, <i>a</i>, <i>--account import</i> } arguments fail when
     * a non-private key is supplied.
     */
    @Test
    @Parameters({"a|import", "a|i", "account|import", "account|i"})
    public void testImportNonPrivateKey(String command, String option) {
        String fakePKey = Hex.toHexString("random".getBytes());

        assertThat(mockCli.call(new String[] {command, option, fakePKey}, cfg)).isEqualTo(ERROR);

        assertThat(mockCli.call(new String[] {command, option, "hello"}, cfg)).isEqualTo(ERROR);
    }

    /** Parameters for testing {@link #testCheckArguments(String[], TaskPriority, Set<String>)}. */
    @SuppressWarnings("unused")
    private Object parametersForArgumentCheck() {
        List<Object> parameters = new ArrayList<>();

        String[] input;
        Set<String> skippedTasks;

        input = new String[] {"--info"};
        skippedTasks = new HashSet<>();
        parameters.add(new Object[] {input, TaskPriority.INFO, skippedTasks});

        input = new String[] {"-s create", "account","create"};
        skippedTasks = new HashSet<>();
        skippedTasks.add("-s create");
        parameters.add(new Object[] {input, TaskPriority.ACCOUNT, skippedTasks});

        input = new String[] {"--info", "--config", "mainnet", "-s create"};
        skippedTasks = new HashSet<>();
        skippedTasks.add("--info");
        skippedTasks.add("-s create");
        parameters.add(new Object[] {input, TaskPriority.CONFIG, skippedTasks});

        input = new String[] {"--help", "--port", TEST_PORT};
        skippedTasks = new HashSet<>();
        skippedTasks.add("--port");
        parameters.add(new Object[] {input, TaskPriority.HELP, skippedTasks});

        input = new String[] {"--help", "--force-compact", "true"};
        skippedTasks = new HashSet<>();
        skippedTasks.add("--force-compact");
        parameters.add(new Object[] {input, TaskPriority.HELP, skippedTasks});

        input = new String[] {"--help", "--network", "mainnet", "--datadir", dataDirectory};
        skippedTasks = new HashSet<>();
        skippedTasks.add("--network");
        skippedTasks.add("--datadir");
        parameters.add(new Object[] {input, TaskPriority.HELP, skippedTasks});

        input = new String[] {"--version", "--port", TEST_PORT, "--datadir", dataDirectory};
        skippedTasks = new HashSet<>();
        skippedTasks.add("--port");
        skippedTasks.add("--datadir");
        parameters.add(new Object[] {input, TaskPriority.VERSION, skippedTasks});

        input = new String[] {"--version", "-v"};
        skippedTasks = new HashSet<>();
        parameters.add(new Object[] {input, TaskPriority.VERSION, skippedTasks});

        input = new String[] {"--version", "--port", TEST_PORT, "--force-compact", "123", "456"};
        skippedTasks = new HashSet<>();
        skippedTasks.add("--port");
        skippedTasks.add("--force-compact");
        parameters.add(new Object[] {input, TaskPriority.VERSION, skippedTasks});

        input = new String[] {"--prune-blocks", "--state", "FULL"};
        skippedTasks = new HashSet<>();
        skippedTasks.add("--state");
        parameters.add(new Object[] {input, TaskPriority.PRUNE_BLOCKS, skippedTasks});

        input = new String[] {"-h", "-v"};
        skippedTasks = new HashSet<>();
        skippedTasks.add("-v");
        parameters.add(new Object[] {input, TaskPriority.HELP, skippedTasks});

        input = new String[] {"-h", "--version"};
        skippedTasks = new HashSet<>();
        skippedTasks.add("--version");
        parameters.add(new Object[] {input, TaskPriority.HELP, skippedTasks});

        input = new String[] {"-h", "-c"};
        skippedTasks = new HashSet<>();
        skippedTasks.add("--config");
        parameters.add(new Object[] {input, TaskPriority.HELP, skippedTasks});

        input = new String[] {"-i", "a", "c"};
        skippedTasks = new HashSet<>();
        skippedTasks.add("account");
        parameters.add(new Object[] {input, TaskPriority.INFO, skippedTasks});

        ECKey key = ECKeyFac.inst().create();
        String pKey = Hex.toHexString(key.getPrivKeyBytes());
        input = new String[] {"-c","mainnet", "account","import", pKey};
        skippedTasks = new HashSet<>();
        skippedTasks.add("account");
        parameters.add(new Object[] {input, TaskPriority.CONFIG, skippedTasks});

        input = new String[] {"-s create", "-r", "100", "pb"};
        skippedTasks = new HashSet<>();
        skippedTasks.add("--revert");
        skippedTasks.add("--prune-blocks");
        parameters.add(new Object[] {input, TaskPriority.SSL, skippedTasks});

        input = new String[] {"-r", "100", "--state", "FULL", "--db-compact"};
        skippedTasks = new HashSet<>();
        skippedTasks.add("--state");
        skippedTasks.add("--db-compact");
        parameters.add(new Object[] {input, TaskPriority.REVERT, skippedTasks});

        input =
                new String[] {
                    "--state", "FULL", "--db-compact"
                };
        skippedTasks = new HashSet<>();
        skippedTasks.add("--db-compact");
        parameters.add(new Object[] {input, TaskPriority.PRUNE_STATE, skippedTasks});

        input = new String[] { "--db-compact","dev","print-state-size","4"};
        skippedTasks = new HashSet<>();
        skippedTasks.add("--db-compact");
        parameters.add(new Object[] {input, TaskPriority.DEV, skippedTasks});
        return parameters.toArray();
    }

    @Test
    @Parameters(method = "parametersForArgumentCheck")
    public void testCheckArguments(
            String[] input, TaskPriority expectedPriority, Set<String> expectedTasks) {
        Arguments options = new Arguments();
        CommandLine parser = new CommandLine(options);
        CommandLine.ParseResult result = parser.parseArgs(input);

        TaskPriority breakingTaskPriority = cli.getBreakingTaskPriority(options, result);
        assertEquals(expectedPriority, breakingTaskPriority);
        Set<String> skippedTasks = cli.getSkippedTasks(options, breakingTaskPriority, result);
        assertEquals(expectedTasks, skippedTasks);
    }

    /**
     * Ensures that the { <i>qt</i>, <i>--query-tx</i>} arguments fail when invalid transaction hash
     * is supplied. Separate 2 tests cause the db has load issue during the multiple parameters
     * input.
     */
    @Test
    @Parameters({"dev qt", "dev query-tx"})
    public void testQueryTransaction(String option) {
        String[] options = option.split("\\s");
        assertThat(mockCli.call(new String[] {options[0], options[1], "0xa0"}, cfg)).isEqualTo(ERROR);
    }

    /**
     * Ensures that the { <i>qb</i>, <i>--query-block</i>} arguments fail when no blocknumber is
     * supplied.
     */
    @Test
    @Parameters({"dev qb", "dev query-block"})
    public void testQueryBlock(String option) {
        String[] options = option.split("\\s");
        assertThat(mockCli.call(new String[] {options[0], options[1], ""}, cfg)).isEqualTo(ERROR);
    }

    /**
     * Ensures that the { <i>qa</i>, <i>--query-account</i>} arguments fail when invalid account
     * address is supplied.
     */
    @Test
    @Parameters({"dev query-account", "dev query-account"})
    public void testQueryAccount(String option) {
        String[] options = option.split("\\s");
        assertThat(mockCli.call(new String[] {options[0], options[1], ""}, cfg)).isEqualTo(ERROR);
    }



    @Test
    @Parameters(method = "parametersForTestEdit")
    public void testEdit(String[] options){
        Arguments args = new Arguments();
        EditCli editCli = new EditCli();

        CommandLine parser = new CommandLine(args).addSubcommand(editCli);
        final CommandLine.ParseResult parseResult = parser.parseArgs(options).subcommand();
        editCli.checkOptions();
        assertThat(parseResult).isNotNull();
        assertThat(parseResult.commandSpec().userObject().getClass()).isEqualTo(EditCli.class);

        assertThat(mockCli.call(options, cfg)).isEqualTo(RUN);
    }

    @Test(expected = RuntimeException.class)
    @Parameters(method = "parametersForTestFailedParse")
    public void testFailedParse(String[] options){
        Arguments args = new Arguments();
        EditCli editCli = new EditCli();

        CommandLine parser = new CommandLine(args).addSubcommand(editCli);
        parser.parseArgs(options).subcommand();
        editCli.checkOptions();
    }

    public Object[] parametersForTestEdit(){
        List<Object> parameters = new ArrayList<>();

        parameters.add("edit port=30000 state-storage=TOP vendor=h2 java=on rpc=off mining=on show-status=on compression=on log GEN=TRACE".split("\\s"));
        List<String[]> parameters2 = new ArrayList<>();
        String[] enableDisableArr = new String[]{"on", "off"};
        String[] settings = new String[]{"java", "rpc", "mining", "show-status","compression"};
        for (int i =0; i<enableDisableArr.length; i++){
            for (int j=0; j<settings.length;j++){
                parameters2.add(new String[]{"edit",settings[j] + "=" + enableDisableArr[i]});
            }
        }

        String[] vendors = new String[]{"h2", "leveldb", "rocksdb"};
        for (int i = 0; i< vendors.length; i++){
            parameters.add(new String[]{"edit", "vendor="+vendors[i]});
        }

        for (int i=0; i< LogLevel.values().length; i++){
            StringBuilder builder = new StringBuilder();
            builder.append("edit log ");
            for(int j=0; j< LogEnum.values().length; j++){
                builder.append(LogEnum.values()[j].name().toLowerCase())
                        .append("=")
                        .append(LogLevel.values()[i].name().toLowerCase()).append(" ");

            }

            parameters2.add(builder.toString().trim().split("\\s"));
        }


        for (int i = 0; i< CfgDb.PruneOption.values().length; i++){
            parameters.add(new String[]{"edit", "state-storage="+ CfgDb.PruneOption.values()[i].name().toLowerCase()});
        }
        parameters.addAll(parameters2);
        parameters.add("edit port=30000 state-storage=TOP vendor=h2 java=on rpc=off mining=on show-status=on compression=on log GEN=TRACE CONS=TRACE SYNC=TRACE API=TRACE VM=TRACE NET=TRACE DB=TRACE P2P=TRACE".split("\\s"));
        parameters.add("-n mainnet edit port=30000 state-storage=TOP vendor=h2 java=on rpc=off mining=on show-status=off compression=on log GEN=TRACE CONS=TRACE SYNC=TRACE API=TRACE VM=TRACE NET=TRACE DB=TRACE P2P=TRACE".split("\\s"));

        return parameters.toArray();
    }

    public Object[] parametersForTestFailedParse(){
        List<Object> parameters = new ArrayList<>();

        parameters.add("edit port=30000 state-storage=TOP -n mainnet vendor=h2 java=on rpc=off mining=on show-status=on compression=on log GEN=TRACE".split("\\s"));
        parameters.add("edit port=30000 state-storage=TOP vendor=h2 java=on rpc=off mining=on show-status=on compression=on log GEN=TRACE CONS=TRACE SYNC=TRACE API=TRACE VM=TRACE NET=TRACE DB=TRACE P2P=TRACE -n mainnet".split("\\s"));
        List<String[]> parameters2 = new ArrayList<>();
        String[] settings = new String[]{"java", "rpc", "mining", "show-status","compression", "port", "state-storage", "vendor", "log"};
        for (int j=0; j<settings.length;j++){
            parameters2.add(new String[]{"edit",settings[j]});
        }

        parameters.add("edit port=-100".split("\\n"));
        parameters.add("edit port=10000000000".split("\\n"));
        parameters.add("edit");

        parameters.addAll(parameters2);
        return parameters.toArray();
    }

    @Parameters(method = "parametersFortestCommandSpec")
    @Test
    public void testCommandSpec(String[] args, Object[] expectedClasses){
        CommandLine cli = new CommandLine(new Arguments()).addSubcommand("edit",new EditCli());

        CommandLine.ParseResult result = cli.parseArgs(args);

        for(Object expected: expectedClasses){
            CommandLine.Model.CommandSpec spec = Cli.findCommandSpec(result, (Class<?>) expected);
            assertThat(spec).isNotNull();
            assertThat(spec.userObject().getClass()).isEqualTo(expected);
        }

    }

    @Parameters(method = "parametersForTestCommandSpecFail")
    @Test(expected = RuntimeException.class)
    public void testCommandSpecFail(String[] args, Object[] expectedClasses){
        CommandLine cli = new CommandLine(new Arguments()).addSubcommand("edit",new EditCli());

        CommandLine.ParseResult result = cli.parseArgs(args);

        for(Object expected: expectedClasses){
            CommandLine.Model.CommandSpec spec = Cli.findCommandSpec(result, (Class<?>) expected);
            Objects.requireNonNull(spec);
        }

    }

    public Object parametersForTestCommandSpecFail(){
        Object[] arr1 = new Object[]{"editt port=1000 devv --help".split("\\s"), List.of(EditCli.class, DevCLI.class).toArray()};
        Object[] arr2 = new Object[]{"DEV --help".split("\\s"), List.of(DevCLI.class).toArray()};
        Object[] arr3 = new Object[]{"EDIT port=1000".split("\\s"), List.of(EditCli.class).toArray()};
        Object[] arr4 = new Object[]{"dev --help".split("\\s"), List.of(DevCLI.class, EditCli.class).toArray()};
        Object[] arr5 = new Object[]{"edit port=1000".split("\\s"), List.of( EditCli.class, DevCLI.class).toArray()};
        Object[] arr6 = new Object[]{"".split("\\s"), List.of(DevCLI.class, EditCli.class).toArray()};
        Object[] arr7 = new Object[]{"".split("\\s"), List.of(EditCli.class, DevCLI.class).toArray()};
        Object[] arr8 = new Object[]{"dev --help edit port=1000".split("\\s"), List.of(EditCli.class, DevCLI.class).toArray()};

        return List.of(arr1, arr2, arr3, arr4, arr5, arr6, arr7, arr8).toArray();
    }

    public Object parametersFortestCommandSpec(){
        Object[] arr1 = new Object[]{"edit port=1000 dev --help".split("\\s"), List.of(EditCli.class, DevCLI.class).toArray()};
        Object[] arr2 = new Object[]{"dev --help".split("\\s"), List.of(DevCLI.class).toArray()};
        Object[] arr3 = new Object[]{"edit port=1000".split("\\s"), List.of(EditCli.class).toArray()};

        return List.of(arr1, arr2, arr3).toArray();
    }

    @Parameters(method = "parametersForTestParseDev")
    @Test
    public void  testParseDev(String[] command){
        CommandLine commandLine  = new CommandLine(new Arguments()).addSubcommand(EditCli.class);
        final CommandLine.ParseResult result = commandLine.parseArgs(command);

        CommandLine.Model.CommandSpec spec = Cli.findCommandSpec(result, DevCLI.class);

        assertThat(spec).isNotNull();
        assertThat(spec.userObject()).isNotNull();
        assertThat(((DevCLI) spec.userObject()).getArgs()).isNotNull();
        ((DevCLI) spec.userObject()).getArgs().checkOptions();
    }



    public Object parametersForTestParseDev(){
        String[] options = new String[]{"-h", "--help", "write-blocks=1","wb=1",  "cs=1","print-state-size=1", "print-state=1","ps=1",
                "print-for-test 10 true str1 str2", "pf 10 true str1 str2","pb=4","print-block=4", "pt=str1", "print-tx=str2", "pa=str1", "print-account=str2", "xs=10", "stop-at=10"};

        String[] commands = new String[]{"edit port 100 dev", "dev", "d"};
        List<String[]> strings = new ArrayList<>();
        for (String option: options){
            for (String command: commands){
                String tmp = command + " "+ option;
                strings.add(tmp.split("\\s"));
            }
        }
        return strings.toArray();
    }

    /*
    Ensure that the dev command only accepts one param within the mutually exclusive group of params
     */
    @Parameters(method = "parametersForTestParseDevFail")
    @Test(expected = RuntimeException.class)
    public void testParseDevFail(String[] arr){
        CommandLine commandLine  = new CommandLine(new Arguments()).addSubcommand(EditCli.class);
        commandLine.parseArgs(arr);
    }


    public Object[] parametersForTestParseDevFail(){
        String[] options = new String[]{"-h", "--help", "write-blocks=1","wb=1",  "cs=1","print-state-size=1", "print-state=1","ps=1",
            "print-for-test 10 true str1 str2", "pt 10 true str1 str2","pb=4","print-block=4", "pt=str1", "print-tx=str2", "pa=str1", "print-account=str2", "xs=10", "stop-at=10"};

        String[] commands = new String[]{"edit port 100 dev", "dev"};
        List<String[]> strings = new ArrayList<>();
        for (String option: options){
            for (String command: commands) {
                for (String option1: options) {
                    String tmp = command + " " + option + " " + option1;
                    strings.add(tmp.split("\\s"));

                }
            }
        }
        strings.add(new String[]{"dev"});
        strings.add("edit port 100 dev".split("\\s"));

        return strings.toArray();
    }

    @Parameters(method = "parametersFortestAccountCliParseAndRun")
    @Test
    public void testAccountCliParseAndRun(String[] options, ReturnType expected ){

        CommandLine commandLine = new CommandLine(Arguments.class).addSubcommand(EditCli.class);
        ParseResult result;
        try{
            result = commandLine.parseArgs(options);
        }catch (Exception e){
            if (expected.equals(ERROR)) {
                return;
            }
            else {
                throw e;
            }
        }
        CommandSpec commandSpec = Cli.findCommandSpec(result, AccountCli.class);

        //noinspection ConstantConditions
        AccountCli spiedAccountCLI = spy((AccountCli) commandSpec.userObject());
        doReturn(true).when(spiedAccountCLI).exportPrivateKey(anyString(),any());
        doReturn(true).when(spiedAccountCLI).listAccounts();
        doReturn(true).when(spiedAccountCLI).createAccount(any());
        doReturn(true).when(spiedAccountCLI).importPrivateKey(anyString(), any());
        doCallRealMethod().when(spiedAccountCLI).runCommand(any());
        assertThat(spiedAccountCLI.runCommand(mockpr)).isEqualTo(expected);
        if (spiedAccountCLI.getList()) {
            verify(spiedAccountCLI, times(1)).listAccounts();
        } else {
            verify(spiedAccountCLI, times(0)).listAccounts();
        }

        if (!spiedAccountCLI.getGroup().getImportAcc().isEmpty()) {
            verify(spiedAccountCLI, times(1)).importPrivateKey(anyString(), any());
        } else {
            verify(spiedAccountCLI, times(0)).importPrivateKey(anyString(),any());
        }

        if (!spiedAccountCLI.getGroup().getExport().isEmpty()) {
            verify(spiedAccountCLI, times(1)).exportPrivateKey(anyString(),any());
        } else {
            verify(spiedAccountCLI, times(0)).exportPrivateKey(anyString(), any());
        }

        if (spiedAccountCLI.getGroup().getCreate()) {
            verify(spiedAccountCLI, times(1)).createAccount(any());
        } else {
            verify(spiedAccountCLI, times(0)).createAccount(any());
        }
        Mockito.clearInvocations(spiedAccountCLI);
    }

    public Object parametersFortestAccountCliParseAndRun(){
        List<Object[]> params = new ArrayList<>();

        List<String> exportList = List.of("","export validstring","e validstring");
        List<String> importList = List.of("", "import validString", "i validString");
        List<String> create = List.of("", "create", "c");
        List<String> list = List.of("", "list", "l");

        for (String e: exportList){
            for (String i: importList){
                for (String c: create){
                    for (String l : list) {
                        String options = String.format("%s %s %s %s", e, i, c, l);

                        ReturnType expected;
                        if(options.trim().isEmpty()){
                            expected = ERROR;
                        } else if (((e.isEmpty() && i.isEmpty())
                                || (e.isEmpty() && c.isEmpty())
                                || (i.isEmpty() && c.isEmpty()))) {
                            expected = EXIT;
                        } else {
                            expected = ERROR;
                        }
                        String[] commands = new String[] {"a", "-a", "account", "--account"};
                        for (String command : commands) {
                            params.add(
                                    new Object[] {
                                        Arrays.stream((command + " " + options).trim().split("\\s"))
                                                .filter(s -> !s.isEmpty())
                                                .toArray(String[]::new),
                                        expected
                                    });
                        }
                    }
                }
            }
        }
        return params.toArray();
    }

    // Methods below taken from FileUtils class
    private static boolean copyRecursively(File src, File target) {
        if (src.isDirectory()) {
            return copyDirectoryContents(src, target);
        } else {
            try {
                Files.copy(src, target);
                return true;
            } catch (IOException e) {
                return false;
            }
        }
    }

    private static boolean copyDirectoryContents(File src, File target) {
        Preconditions.checkArgument(src.isDirectory(), "Source dir is not a directory: %s", src);

        // Don't delete symbolic link directories
        if (isSymbolicLink(src)) {
            return false;
        }

        target.mkdirs();
        Preconditions.checkArgument(target.isDirectory(), "Target dir is not a directory: %s", src);

        boolean success = true;
        for (File file : listFiles(src)) {
            success = copyRecursively(file, new File(target, file.getName())) && success;
        }
        return success;
    }

    private static boolean isSymbolicLink(File file) {
        try {
            File canonicalFile = file.getCanonicalFile();
            File absoluteFile = file.getAbsoluteFile();
            File parentFile = file.getParentFile();
            // a symbolic link has a different name between the canonical and absolute path
            return !canonicalFile.getName().equals(absoluteFile.getName())
                    ||
                    // or the canonical parent path is not the same as the file's parent path,
                    // provided the file has a parent path
                    parentFile != null
                            && !parentFile.getCanonicalPath().equals(canonicalFile.getParent());
        } catch (IOException e) {
            // error on the side of caution
            return true;
        }
    }

    private static ImmutableList<File> listFiles(File dir) {
        File[] files = dir.listFiles();
        if (files == null) {
            return ImmutableList.of();
        }
        return ImmutableList.copyOf(files);
    }

    private static boolean deleteRecursively(File file) {
        Path path = file.toPath();
        try {
            java.nio.file.Files.walkFileTree(
                    path,
                    new SimpleFileVisitor<>() {
                        @Override
                        public FileVisitResult visitFile(
                                final Path file, final BasicFileAttributes attrs)
                                throws IOException {
                            java.nio.file.Files.delete(file);
                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult visitFileFailed(
                                final Path file, final IOException e) {
                            return handleException(e);
                        }

                        private FileVisitResult handleException(final IOException e) {
                            // e.printStackTrace();
                            return FileVisitResult.TERMINATE;
                        }

                        @Override
                        public FileVisitResult postVisitDirectory(
                                final Path dir, final IOException e) throws IOException {
                            if (e != null) {
                                return handleException(e);
                            }
                            java.nio.file.Files.delete(dir);
                            return FileVisitResult.CONTINUE;
                        }
                    });
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }
}
