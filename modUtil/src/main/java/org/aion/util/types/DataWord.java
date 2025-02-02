package org.aion.util.types;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.aion.util.bytes.ByteUtil;
import org.aion.util.conversions.Hex;

/**
 * Data word is the basic unit data used by virtual machine. The size of a data word is 128 bits.
 */
public class DataWord {

    public static final BigInteger MAX_VALUE =
            BigInteger.valueOf(2).pow(128).subtract(BigInteger.ONE);

    public static final DataWord ZERO = new DataWord(0);
    public static final DataWord ONE = new DataWord(1);
    public static final int BYTES = 16;

    private byte[] data;

    public DataWord() {
        data = new byte[BYTES];
    }

    public DataWord(int num) {
        ByteBuffer bb = ByteBuffer.allocate(BYTES);
        bb.position(12);
        bb.putInt(num);
        data = bb.array();
    }

    public DataWord(long num) {
        ByteBuffer bb = ByteBuffer.allocate(BYTES);
        bb.position(8);
        bb.putLong(num);
        data = bb.array();
    }

    public DataWord(byte[] data) {
        if (data == null) {
            throw new NullPointerException("Input data");
        } else if (data.length == BYTES) {
            this.data = Arrays.copyOf(data, data.length);
        } else if (data.length < BYTES) {
            this.data = new byte[BYTES];
            System.arraycopy(data, 0, this.data, BYTES - data.length, data.length);
        } else {
            throw new RuntimeException("Data word can't exceed 16 bytes: " + Hex.toHexString(data));
        }
    }

    public DataWord(BigInteger num) {
        // NOTE: DataWordImpl.value() produces a signed positive BigInteger. The byte array
        // representation of such a number must prepend a zero byte so that this can be decoded
        // correctly. This means that a 16-byte array with a non-zero starting bit will become 17
        // bytes when BigInteger::toByteArray is called, and therefore we must remove any leading
        // zero bytes from this representation for full compatibility.
        this(removeLargeBigIntegerLeadingZeroByte(num));
    }

    /**
     * Similar to {@code stripLeadingZeroes} but more specialized to be more efficient in a specific
     * necessary situation.
     *
     * <p>Essentially this method will always return {@code number.toByteArray()} UNLESS this byte
     * array is length {@value BYTES} + 1 (that is, 17), and its initial byte is a zero byte. In
     * this single case, the leading zero byte will be stripped.
     *
     * @param number The {@link BigInteger} whose byte array representation is to be possibly
     *     truncated.
     * @return The re-formatted {@link BigInteger#toByteArray()} representation as here specified.
     */
    private static byte[] removeLargeBigIntegerLeadingZeroByte(BigInteger number) {
        byte[] bytes = number.toByteArray();
        return ((bytes.length == (DataWord.BYTES + 1)) && (bytes[0] == 0x0))
                ? Arrays.copyOfRange(bytes, 1, bytes.length)
                : bytes;
    }

    public DataWord(String data) {
        this(Hex.decode(data));
    }

    public DataWord(ByteArrayWrapper wrapper) {
        this(wrapper.getData());
    }

    public byte[] getData() {
        return data;
    }

    public byte[] getNoLeadZeroesData() {
        return ByteUtil.stripLeadingZeroes(data);
    }

    public BigInteger value() {
        return new BigInteger(1, data);
    }

    public int intValue() {
        int v = 0;
        for (int i = 12; i < BYTES; i++) {
            v = (v << 8) + (data[i] & 0xff);
        }

        return v;
    }

    public long longValue() {
        long v = 0;
        for (int i = 8; i < BYTES; i++) {
            v = (v << 8) + (data[i] & 0xff);
        }

        return v;
    }

    public boolean isZero() {
        for (int i = 0; i < BYTES; i++) {
            if (data[BYTES - 1 - i] != 0) {
                return false;
            }
        }
        return true;
    }

    public boolean isNegative() {
        int result = data[0] & 0x80;
        return result == 0x80;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DataWord dataWord = (DataWord) o;

        return Arrays.equals(data, dataWord.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

    @Override
    public String toString() {
        return Hex.toHexString(data);
    }

    public ByteArrayWrapper toWrapper() {
        return ByteArrayWrapper.wrap(data);
    }
}
