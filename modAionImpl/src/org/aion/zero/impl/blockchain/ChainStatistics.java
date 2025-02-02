package org.aion.zero.impl.blockchain;

import java.util.LinkedList;
import java.util.List;

/** Chain statistics. */
public class ChainStatistics {

    private static final int ExecTimeListLimit = 10000;

    private long startupTimeStamp;
    private boolean consensus = true;
    private List<Long> blockExecTime = new LinkedList<>();

    public void init() {
        startupTimeStamp = System.currentTimeMillis();
    }

    public long getStartupTimeStamp() {
        return startupTimeStamp;
    }

    public boolean isConsensus() {
        return consensus;
    }

    public void lostConsensus() {
        consensus = false;
    }

    public void addBlockExecTime(long time) {
        while (blockExecTime.size() > ExecTimeListLimit) {
            blockExecTime.remove(0);
        }
        blockExecTime.add(time);
    }

    public Long getExecAvg() {

        if (blockExecTime.isEmpty()) {
            return 0L;
        }

        long sum = 0;
        for (int i = 0; i < blockExecTime.size(); ++i) {
            sum += blockExecTime.get(i);
        }

        return sum / blockExecTime.size();
    }

    public List<Long> getBlockExecTime() {
        return blockExecTime;
    }
}
