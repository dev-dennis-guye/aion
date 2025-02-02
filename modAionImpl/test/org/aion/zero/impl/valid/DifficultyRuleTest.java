package org.aion.zero.impl.valid;

import static com.google.common.truth.Truth.assertThat;
import static org.aion.zero.impl.types.AbstractBlockHeader.NONCE_LENGTH;
import static org.aion.zero.impl.types.AbstractBlockHeader.SOLUTIONSIZE;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.util.LinkedList;
import java.util.List;
import org.aion.util.bytes.ByteUtil;
import org.aion.zero.impl.core.IDifficultyCalculator;
import org.aion.util.types.AddressUtils;
import org.aion.zero.impl.blockchain.ChainConfiguration;
import org.aion.zero.impl.types.A0BlockHeader;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests for {@link GrandParentDependantBlockHeaderRule}.
 *
 * @author Jay Tseng
 */
public class DifficultyRuleTest {
    private A0BlockHeader grandParentHeader;
    private A0BlockHeader parentHeader;
    private A0BlockHeader currentHeader;
    @Mock ChainConfiguration mockChainCfg;
    @Mock IDifficultyCalculator mockDiffCalculator;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        grandParentHeader =
                new A0BlockHeader(
                        (byte) 0x01,
                        1,
                        new byte[32],
                        AddressUtils.ZERO_ADDRESS,
                        new byte[256],
                        ByteUtil.intToBytes(1),
                        null,
                        1,
                        1,
                        1,
                        new byte[NONCE_LENGTH],
                        new byte[SOLUTIONSIZE]);
        parentHeader =
                new A0BlockHeader(
                        (byte) 0x01,
                        2,
                        new byte[32],
                        AddressUtils.ZERO_ADDRESS,
                        new byte[256],
                        ByteUtil.intToBytes(1),
                        null,
                        1,
                        1,
                        11,
                        new byte[NONCE_LENGTH],
                        new byte[SOLUTIONSIZE]);
    }

    /**
     * Checks if the {@link GrandParentDependantBlockHeaderRule#validate} returns {@code false} when the difficulty
     * data is <b>greater</b> than the difficulty data length.
     */
    @Test
    public void testInvalidDifficultyLength() {

        currentHeader =
                new A0BlockHeader(
                        (byte) 0x01,
                        3,
                        new byte[32],
                        AddressUtils.ZERO_ADDRESS,
                        new byte[256],
                        new byte[17],
                        null,
                        1,
                        1,
                        3,
                        new byte[NONCE_LENGTH],
                        new byte[SOLUTIONSIZE]);

        List<RuleError> errors = new LinkedList<>();

        // generate output
        boolean actual =
                new AionDifficultyRule(mockChainCfg)
                        .validate(grandParentHeader, parentHeader, currentHeader, errors);

        // test output
        assertThat(actual).isFalse();
    }

    /**
     * Checks if the {@link GrandParentDependantBlockHeaderRule#validate} returns {@code true} when the difficulty
     * data is <b>equal</b> the difficulty data length.
     */
    @Test
    public void testDifficultyLength() {

        currentHeader =
                new A0BlockHeader(
                        (byte) 0x01,
                        3,
                        new byte[32],
                        AddressUtils.ZERO_ADDRESS,
                        new byte[256],
                        ByteUtil.bigIntegerToBytes(BigInteger.ONE, 16),
                        null,
                        1,
                        1,
                        3,
                        new byte[NONCE_LENGTH],
                        new byte[SOLUTIONSIZE]);

        List<RuleError> errors = new LinkedList<>();

        when(mockChainCfg.getDifficultyCalculator()).thenReturn(mockDiffCalculator);
        when(mockDiffCalculator.calculateDifficulty(parentHeader, grandParentHeader))
                .thenReturn(BigInteger.ONE);

        // generate output
        boolean actual =
                new AionDifficultyRule(mockChainCfg)
                        .validate(grandParentHeader, parentHeader, currentHeader, errors);

        // test output
        assertThat(actual).isTrue();
    }
}
