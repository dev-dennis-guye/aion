package org.aion.mcf.ds;

import org.aion.db.impl.ByteArrayKeyValueDatabase;
import org.aion.util.types.ByteArrayWrapper;
import org.slf4j.Logger;

/**
 * Adds debug messages and cache utilization information to {@link LruDataSource}.
 *
 * @author Alexandra Roatis
 */
public final class DebugLruDataSource<V> extends LruDataSource<V> {

    // used to gather information regarding the cache use
    private long hits;
    private long missed;

    // for printing debug information
    private Logger log;

    // only DataSource should know about this implementation
    DebugLruDataSource(ByteArrayKeyValueDatabase src, Serializer<V, byte[]> serializer, int cacheSize, Logger log) {
        super(src, serializer, cacheSize);

        this.hits = 0L;
        this.missed = 0L;
        this.log = log;
    }

    public V get(byte[] key) {
        ByteArrayWrapper wrappedKey = ByteArrayWrapper.wrap(key);
        if (cache.containsKey(wrappedKey)) {
            // gather usage data
            hits++;

            return cache.get(wrappedKey);
        } else {
            // gather usage data
            missed++;

            V val = super.get(key);
            cache.put(wrappedKey, val);

            // logging information on missed caching opportunities
            if (log.isTraceEnabled()) {
                log.trace("[Database:" + getName() + "] Stack trace for missed cache retrieval: ", new Exception());
            }

            return val;
        }
    }

    @Override
    public void close() {
        super.close();

        // log statistics at shutdown
        log.info("[Database:" + getName() + "] Cache utilization: hits = " + hits + " misses = " + missed);
    }

    private String getName() {
        return getSrc().getName().orElse("UNKNOWN");
    }
}
