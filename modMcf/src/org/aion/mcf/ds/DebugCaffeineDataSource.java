package org.aion.mcf.ds;

import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.util.concurrent.TimeUnit;
import org.aion.db.impl.ByteArrayKeyValueDatabase;
import org.aion.util.types.ByteArrayWrapper;
import org.slf4j.Logger;

/**
 * Adds a Window-TinyLfu cache of predefined size to the {@link ObjectDataSource}. For <a
 * href=https://github.com/ben-manes/caffeine/wiki/Efficiency>efficiency details</a> regarding the
 * {@link Caffeine}.
 *
 * @author Alexandra Roatis
 */
public class DebugCaffeineDataSource<V> extends ObjectDataSource<V> {

    protected final LoadingCache<ByteArrayWrapper, V> cache;

    // for printing debug information
    private Logger log;

    // only DataSource should know about this implementation
    DebugCaffeineDataSource(ByteArrayKeyValueDatabase src, Serializer<V, byte[]> serializer, int cacheSize, Logger log) {
        super(src, serializer);
        this.log = log;

        // build with recordStats
        if (this.log.isTraceEnabled()) {
            this.cache =
                    Caffeine.newBuilder()
                            .expireAfterWrite(6, TimeUnit.MINUTES)
                            .maximumSize(cacheSize)
                            .recordStats()
                            .build(
                                    key -> {
                                        // logging information on missed caching opportunities
                                        if (this.log.isTraceEnabled()) {
                                            this.log.trace("[Database:" + getName() + "] Stack trace for missed cache retrieval: ", new Exception());
                                        }

                                        return getFromDatabase(key.getData());
                                    });
        } else {
            this.cache =
                    Caffeine.newBuilder()
                            .expireAfterWrite(6, TimeUnit.MINUTES)
                            .maximumSize(cacheSize)
                            .recordStats()
                            .build(key -> getFromDatabase(key.getData()));
        }
    }

    public void put(byte[] key, V value) {
        super.put(key, value);
        cache.put(ByteArrayWrapper.wrap(key), value);
    }

    public void putToBatch(byte[] key, V value) {
        super.putToBatch(key, value);
        cache.put(ByteArrayWrapper.wrap(key), value);
    }

    public void deleteInBatch(byte[] key) {
        super.deleteInBatch(key);
        cache.invalidate(ByteArrayWrapper.wrap(key));
    }

    public void delete(byte[] key) {
        super.delete(key);
        cache.invalidate(ByteArrayWrapper.wrap(key));
    }

    public V get(byte[] key) {
        return cache.get(ByteArrayWrapper.wrap(key));
    }

    @Override
    public void close() {
        super.close();

        // log statistics at shutdown
        log.info("[Database:" + getName() + "] Cache utilization: " + cache.stats());
    }

    private String getName() {
        return getSrc().getName().orElse("UNKNOWN");
    }
}
