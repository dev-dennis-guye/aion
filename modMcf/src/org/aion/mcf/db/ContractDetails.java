package org.aion.mcf.db;

import java.util.Collection;
import java.util.Map;
import org.aion.types.AionAddress;
import org.aion.util.types.ByteArrayWrapper;

public interface ContractDetails {

    /**
     * Inserts a key-value pair containing the given key and the given value.
     *
     * @param key the key to be inserted
     * @param value the value to be inserted
     */
    void put(ByteArrayWrapper key, ByteArrayWrapper value);

    /**
     * Deletes any key-value pair that matches the given key.
     *
     * @param key the key to be deleted
     */
    void delete(ByteArrayWrapper key);

    /**
     * Returns the value associated with key.
     *
     * @implNote Some implementations may handle a non-existent key-value pair differently.
     * @param key The key to query.
     * @return The associated value or some non-value indicator in the case of no such key-value
     *     pair.
     */
    ByteArrayWrapper get(ByteArrayWrapper key);

    /**
     * Returns the code of the address associated with this ContractDetails class. This is for
     * addresses that are smart contracts.
     *
     * @return the code of the associated address.
     */
    byte[] getCode();

    /**
     * Returns the code whose hash is codeHash.
     *
     * @param codeHash The hashed code.
     * @return the code.
     */
    byte[] getCode(byte[] codeHash);

    /**
     * Sets the code of the associated address to code.
     *
     * @param code The code to set.
     */
    void setCode(byte[] code);

    /**
     * Returns the transformed code of the vm contract by giving the codeHash of the deploy code.
     *
     * @return the transformed jvm byte code.
     */
    byte[] getTransformedCode();

    /**
     * Puts the transformed code of the vm contract by giving the codeHash of the deploy code.
     *
     * @param transformedCode the code of the transformed code.
     */
    void setTransformedCode(byte[] transformedCode);

    /**
     * Returns the transaction type used to deploy the contract indicating which VM was used.
     *
     * @return the transaction type used to deploy the contract indicating which VM was used
     */
    InternalVmType getVmType();

    /**
     * Sets the transaction type value used to deploy the contract symbolizing the VM that manages
     * the contract.
     *
     * @param vmType the transaction type value used to deploy the contract symbolizing the VM that
     *     manages the contract
     */
    void setVmType(InternalVmType vmType);

    /**
     * Returns a byte array from contract storage representing an encoding of the object graph for
     * the given contract.
     *
     * @return a byte array from contract storage representing an encoding of the object graph for
     *     the given contract
     */
    byte[] getObjectGraph();

    /**
     * Saves the object graph for the given contract into contract storage.
     *
     * @param graph a byte array representing an encoding of the object graph for the given contract
     */
    void setObjectGraph(byte[] graph);

    /**
     * Returns the storage hash.
     *
     * @return the storage hash.
     */
    byte[] getStorageHash();

    /**
     * Sets the dirty value to dirty.
     *
     * @param dirty The dirty value.
     */
    void setDirty(boolean dirty);

    /**
     * Sets the deleted value to deleted.
     *
     * @param deleted the deleted value.
     */
    void setDeleted(boolean deleted);

    /**
     * Returns true iff the ContractDetails is dirty.
     *
     * @return only if this is dirty.
     */
    boolean isDirty();

    /**
     * Returns true iff the ContractDetails is deleted.
     *
     * @return only if this is deleted.
     */
    boolean isDeleted();

    /**
     * Returns a mapping of all the key-value pairs that have keys in the given collection keys.
     *
     * @param keys the keys to query for
     * @return the associated mappings
     */
    Map<ByteArrayWrapper, ByteArrayWrapper> getStorage(Collection<ByteArrayWrapper> keys);

    /**
     * Sets the storage to contain the specified key-value mappings.
     *
     * @param storage the specified mappings
     * @apiNote Used for testing.
     * @implNote A {@code null} value is interpreted as deletion.
     */
    void setStorage(Map<ByteArrayWrapper, ByteArrayWrapper> storage);

    /**
     * Get the address associated with this ContractDetails.
     *
     * @return the associated address.
     */
    AionAddress getAddress();

    /**
     * Sets the associated address to address.
     *
     * @param address The address to set.
     */
    void setAddress(AionAddress address);

    /**
     * Returns a string representation of this ContractDetails.
     *
     * @return a string representation.
     */
    String toString();

    /** Syncs the storage trie. */
    void syncStorage();

    /**
     * Returns a sufficiently deep copy of this object. It is up to all implementations of this
     * method to declare which original object references are in fact leaked by this copy, if any,
     * and to provide justification of why, despite this, the copy is nonetheless sufficiently deep.
     *
     * @return A copy of this object.
     */
    ContractDetails copy();
}
