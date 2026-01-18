package eu.solven.adhoc.fsst;

import java.io.ByteArrayOutputStream;

/**
 * Knows how to encodes `byte[]` given a pre-trained {@link SymbolTable}.
 * 
 * @author Benoit Lacelle
 */
public final class FsstEncoder {

	private final SymbolTable table;

	public FsstEncoder(SymbolTable table) {
		this.table = table;
	}

	public byte[] encode(byte[] input) {
		ByteArrayOutputStream out = new ByteArrayOutputStream(input.length);
		int i = 0;

		while (i < input.length) {
			Symbol s = table.find(input, i, Math.min(8, input.length - i));
			if (s != null) {
				// symbols are represented by a code, which has 0 as higher bit, as they go from 0 to 127
				out.write(s.getCode());
				i += s.getKey().length();
			} else {
				// else plain byte

				if ((input[i] & 0x80) == 0x80) {
					// input conflicts with the mask: we write an escape character
					out.write(0xFF);
				}

				out.write((input[i] & 0xFF) | 0x80); // 128..255
				i++;
			}
		}
		return out.toByteArray();
	}
}