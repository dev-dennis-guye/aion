package org.aion.zero.impl.db;

import static org.aion.crypto.HashUtil.EMPTY_DATA_HASH;
import static org.aion.crypto.HashUtil.h256;
import static org.aion.util.bytes.ByteUtil.EMPTY_BYTE_ARRAY;

import com.google.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.aion.mcf.db.ContractDetails;
import org.aion.mcf.db.InternalVmType;
import org.aion.util.conversions.Hex;
import org.aion.util.types.ByteArrayWrapper;

/** Abstract contract details. */
public abstract class AbstractContractDetails implements ContractDetails {

    private boolean dirty = false;
    private boolean deleted = false;

    // a value > 0 indicates that prune should be for that many blocks.
    protected int prune = 0;
    // indicates the maximum storage size before shifting to the storage database
    // NOTE: updating this value can lead to incompatible data storage
    protected int detailsInMemoryStorageLimit = 64 * 1024;

    private Map<ByteArrayWrapper, byte[]> codes = new HashMap<>();
    protected byte[] transformedCode;
    // classes extending this rely on this value starting off as null
    protected byte[] objectGraph = null;

    // using the default transaction type to specify undefined VM
    protected InternalVmType vmType = InternalVmType.EITHER;

    @Override
    public byte[] getCode() {
        return codes.size() == 0 ? EMPTY_BYTE_ARRAY : codes.values().iterator().next();
    }

    @Override
    public byte[] getCode(byte[] codeHash) {
        if (java.util.Arrays.equals(codeHash, EMPTY_DATA_HASH)) {
            return EMPTY_BYTE_ARRAY;
        }
        byte[] code = codes.get(new ByteArrayWrapper(codeHash));
        return code == null ? EMPTY_BYTE_ARRAY : code;
    }

    @Override
    public byte[] getTransformedCode() {
        return transformedCode;
    }

    @Override
    public void setTransformedCode(byte[] transformedCode) {
        // ensures that the object is not set to dirty when copied
        if (!Arrays.equals(this.transformedCode, transformedCode)) {
            this.transformedCode = transformedCode;
            setDirty(true);
        }
    }

    @Override
    public void setCode(byte[] code) {
        if (code == null) {
            return;
        }
        try {
            codes.put(ByteArrayWrapper.wrap(h256(code)), code);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        setDirty(true);
    }

    public Map<ByteArrayWrapper, byte[]> getCodes() {
        return codes;
    }

    protected void setCodes(Map<ByteArrayWrapper, byte[]> codes) {
        this.codes = new HashMap<>(codes);
    }

    public void appendCodes(Map<ByteArrayWrapper, byte[]> codes) {
        this.codes.putAll(codes);
    }

    @Override
    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    @Override
    public boolean isDirty() {
        return dirty;
    }

    @Override
    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    @Override
    public boolean isDeleted() {
        return deleted;
    }

    @Override
    public String toString() {
        String ret;
        ret = "VM: " + vmType.toString();
        ret += " dirty: " + isDirty();

        if (codes != null) {
            ret +=
                    "  Code: "
                            + (codes.size() < 2
                                    ? Hex.toHexString(getCode())
                                    : codes.size() + " versions")
                            + "\n";
        } else {
            ret += "  Code: null\n";
        }

        byte[] storage = getStorageHash();
        if (storage != null) {
            ret += "  Storage: " + Hex.toHexString(storage);
        } else {
            ret += "  Storage: null";
        }

        return ret;
    }

    @VisibleForTesting
    @Override
    public void setStorage(Map<ByteArrayWrapper, ByteArrayWrapper> storage) {
        for (Map.Entry<ByteArrayWrapper, ByteArrayWrapper> entry : storage.entrySet()) {
            ByteArrayWrapper key = entry.getKey();
            ByteArrayWrapper value = entry.getValue();

            if (value != null) {
                put(key, value);
            } else {
                delete(key);
            }
        }
    }

    @Override
    public Map<ByteArrayWrapper, ByteArrayWrapper> getStorage(Collection<ByteArrayWrapper> keys) {
        Map<ByteArrayWrapper, ByteArrayWrapper> storage = new HashMap<>();

        if (keys == null) {
            throw new IllegalArgumentException("Input keys cannot be null");
        } else {
            for (ByteArrayWrapper key : keys) {
                ByteArrayWrapper value = get(key);

                // we check if the value is not null,
                // cause we keep all historical keys
                if (value != null) {
                    storage.put(key, value);
                }
            }
        }

        return storage;
    }
}
