package eu.solven.adhoc.fsst;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Objects;

/**
 * Value object for using byte[] slices as HashMap keys.
 * 
 * @author Benoit Lacelle
 */
final class ByteArrayKey {

	private final byte[] data;
	private final int offset;
	private final int length;
	private final int hash;

	/**
	 * Should not be used, out of through `.read` and `.write`.
	 * 
	 * @param source
	 * @param offset
	 * @param length
	 */
	ByteArrayKey(byte[] source, int offset, int length) {
		this.data = source;
		this.offset = offset;
		this.length = length;
		this.hash = hashCode(this.data, offset, length);
	}

	// BEWARE Is this actually relevant to unroll for performance?
	static int hashCode(byte[] a, int offset, int length) {
		int h = 1;
		int i = offset;
		int end = offset + length;

		while (i + 3 < end) {
			h = 31 * h + a[i++];
			h = 31 * h + a[i++];
			h = 31 * h + a[i++];
			h = 31 * h + a[i++];
		}
		while (i < end) {
			h = 31 * h + a[i++];
		}
		return h;
	}

	/**
	 * Make a ByteArrayKey for a read operation: the source may be referred and modified later
	 * 
	 * @param source
	 * @param offset
	 * @param length
	 * @return
	 */
	public static ByteArrayKey read(byte[] source, int offset, int length) {
		return new ByteArrayKey(source, offset, length);
	}

	/**
	 * Make a ByteArrayKey for a write operation: the data is copied in a fresh `byte[]` as the input source may
	 * modified later while this object has to be immutable.
	 * 
	 * @param source
	 * @param offset
	 * @param length
	 * @return
	 */
	public static ByteArrayKey write(byte[] source, int offset, int length) {
		return new ByteArrayKey(Arrays.copyOfRange(source, offset, offset + length), 0, length);
	}

	int length() {
		return length;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (!(o instanceof ByteArrayKey other))
			return false;

		if (this.length != other.length) {
			return false;
		}
		for (int i = 0; i < length; i++) {
			if (this.data[this.offset + i] != other.data[other.offset + i]) {
				return false;
			}
		}

		return true;
	}

	@Override
	public int hashCode() {
		return hash;
	}

	public void writeBytes(ByteArrayOutputStream out) {
		out.write(data, offset, length);
	}

	@Override
	public String toString() {
		return toHexString(data, offset, length);
	}

	static String toHexString(byte[] a, int offset, int length) {
		Objects.checkFromIndexSize(offset, length, a.length);

		StringBuilder sb = new StringBuilder(length * 2);
		for (int i = offset; i < offset + length; i++) {
			sb.append(String.format("%02x", a[i]));
		}
		return sb.toString();
	}
}