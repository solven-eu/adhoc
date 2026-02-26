package eu.solven.adhoc.encoding.bytes;

import lombok.Builder;

/**
 * Wraps an {@link IByteSlice} while marking it explicitly as UTF-8.
 * 
 * @author Benoit Lacelle
 */
@Builder
public class Utf8ByteSlice implements IByteSlice {
	final IByteSlice byteSlice;

	@Override
	public boolean isFastAsArray() {
		return byteSlice.isFastAsArray();
	}

	@Override
	public byte[] cropped() {
		return byteSlice.cropped();
	}

	@Override
	public byte read(int position) {
		return byteSlice.read(position);
	}

	@Override
	public byte[] array() {
		return byteSlice.array();
	}

	@Override
	public int length() {
		return byteSlice.length();
	}

	@Override
	public int offset() {
		return byteSlice.offset();
	}
}
