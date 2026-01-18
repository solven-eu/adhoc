package eu.solven.adhoc.fsst;

import java.io.ByteArrayOutputStream;

public final class FsstDecoder {

	private final SymbolTable table;

	public FsstDecoder(SymbolTable table) {
		this.table = table;
	}

	public byte[] decode(byte[] compressed) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();

		for (int i = 0; i < compressed.length; i++) {
			byte b = compressed[i];

			int code = b & 0xFF;
			if ((code & 0x80) == 0) {
				// the first bit is 0: this is an encoded symbol
				table.getByCode(code).writeBytes(out);
			} else {
				if (code == 0xFF) {
					// we encounter an escape character: the next byte has to be written as-is

					// BEWARE This modify the loop iterator while inside the loop
					byte escapedByte = compressed[++i];
					out.write(escapedByte);
				} else {
					// literal, we remove the bit from the mask
					out.write(code & 0x7F);
				}
			}
		}
		return out.toByteArray();
	}
}