package org.aion.mcf.ds;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.util.concurrent.TimeUnit;
import org.aion.db.impl.ByteArrayKeyValueDatabase;
import org.aion.util.types.ByteArrayWrapper;

/**
 * Adds a Window-TinyLfu cache of predefined size to the {@link ObjectDataSource}. For <a
 * href=https://github.com/ben-manes/caffeine/wiki/Efficiency>efficiency details</a> regarding the
 * {@link Caffeine}.
 *
 * @author Alexandra Roatis
 */
public final class CaffeineDataSource<V> extends ObjectDataSource<V> {

    private final LoadingCache<ByteArrayWrapper, V> cache;

    // Only DataSource should know about this implementation
    CaffeineDataSource(ByteArrayKeyValueDatabase src, Serializer<V, byte[]> serializer, int cacheSize) {
        super(src, serializer);
        this.cache =
                Caffeine.newBuilder()
                        .maximumSize(cacheSize)
                        .expireAfterWrite(6, TimeUnit.MINUTES)
                        .build(key -> getFromDatabase(key.getData()));
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
}
