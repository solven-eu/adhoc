/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package eu.solven.adhoc.fsst;

import java.io.ByteArrayOutputStream;

/**
 * Decode a `byte[]` to the original `byte[]`
 * 
 * @author Benoit Lacelle
 */
public final class FsstDecoder implements IFsstConstants {

	final SymbolTable table;

	public FsstDecoder(SymbolTable table) {
		this.table = table;
	}

	@SuppressWarnings({ "checkstyle:MagicNumber", "PMD.AssignmentInOperand", "PMD.AvoidReassigningLoopVariables" })
	public byte[] decode(byte[] compressed) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();

		for (int i = 0; i < compressed.length; i++) {
			byte b = compressed[i];

			int code = b & 0xFF;
			if ((code & MASK_NOT_SYMBOL) == 0) {
				// the first bit is 0: this is an encoded symbol
				table.getByCode(code).writeBytes(out);
			} else {
				if (code == ENTRY_ESCAPE) {
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