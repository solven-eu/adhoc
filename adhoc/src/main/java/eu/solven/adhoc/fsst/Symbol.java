package eu.solven.adhoc.fsst;

import java.io.ByteArrayOutputStream;

import lombok.RequiredArgsConstructor;
import lombok.Value;

/**
 * 
 * Represents a symbol, which is a mapping from the original bytes to an encoded code.
 * 
 * @author Benoit Lacelle
 * 
 */
@RequiredArgsConstructor
@Value
public final class Symbol {
	private final ByteArrayKey key;
	private final int code;

	@Override
	public String toString() {
		return code + ":" + key;
	}

	public void writeBytes(ByteArrayOutputStream out) {
		key.writeBytes(out);
	}
}