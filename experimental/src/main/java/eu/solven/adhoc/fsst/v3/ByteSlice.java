package eu.solven.adhoc.fsst.v3;

import java.util.Arrays;

import lombok.AllArgsConstructor;

/**
 * A byte wrapper, enabling some sub-byte[] without creating a new array.
 * 
 * @author Benoit Lacelle
 */
@AllArgsConstructor
public final class ByteSlice {
	final byte[] array;
	final int offset;
	final int length;

	/**
	 * 
	 * @return a non-defensive copy of this as a `byte[]`.
	 */
	public byte[] asByteArray() {
		if (offset == 0 && array.length == length) {
			return array;
		} else {
			return Arrays.copyOfRange(array, offset, offset + length);
		}
	}
}