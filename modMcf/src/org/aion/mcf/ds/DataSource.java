package org.aion.mcf.ds;

import org.aion.db.impl.ByteArrayKeyValueDatabase;
import org.aion.log.AionLoggerFactory;
import org.aion.log.LogEnum;
import org.slf4j.Logger;

/**
 * Builder for different data source implementations.
 *
 * @author Alexandra Roatis
 */
public final class DataSource<V> {

    // Required parameters
    private final ByteArrayKeyValueDatabase src;
    private final Serializer<V, byte[]> serializer;

    // Optional parameters
    private int cacheSize;
    private Type cacheType;
    private boolean isDebug;
    private static final Logger LOG = AionLoggerFactory.getLogger(LogEnum.CACHE.name());

    public enum Type {
        LRU,
        Window_TinyLfu
    }

    public DataSource(ByteArrayKeyValueDatabase src, Serializer<V, byte[]> serializer) {
        this.src = src;
        this.serializer = serializer;
        this.cacheSize = 0;
    }

    public DataSource<V> withCache(int cacheSize, Type cacheType) {
        if (cacheSize == 0) {
            throw new IllegalArgumentException("Please provide a cache size greater than zero.");
        }
        this.cacheSize = cacheSize;
        this.cacheType = cacheType;
        this.isDebug = false;
        return this;
    }

    public DataSource<V> withStatistics() {
        this.isDebug = true;
        return this;
    }

    public ObjectDataSource<V> buildObjectSource() {
        if (cacheSize != 0) {
            if (isDebug) {
                switch (cacheType) {
                    case LRU:
                        return new DebugLruDataSource<>(src, serializer, cacheSize, LOG);
                    case Window_TinyLfu:
                        return new DebugCaffeineDataSource<>(src, serializer, cacheSize, LOG);
                }
            } else {
                switch (cacheType) {
                    case LRU:
                        return new LruDataSource<>(src, serializer, cacheSize);
                    case Window_TinyLfu:
                        return new CaffeineDataSource<>(src, serializer, cacheSize);
                }
            }
        }

        // in case the given cache size is equal to zero
        return new ObjectDataSource<>(src, serializer);
    }

    public DataSourceArray<V> buildArraySource() {
        return new DataSourceArray<>(this.buildObjectSource());
    }
}
