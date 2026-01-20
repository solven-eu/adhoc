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
 * Knows how to encodes `byte[]` given a pre-trained {@link SymbolTable}.
 * 
 * @author Benoit Lacelle
 */
public final class FsstEncoder implements IFsstConstants {

	private final SymbolTable table;

	public FsstEncoder(SymbolTable table) {
		this.table = table;
	}

	public byte[] encode(byte[] input) {
		ByteArrayOutputStream out = new ByteArrayOutputStream(input.length);
		int i = 0;

		while (i < input.length) {
			Symbol s = table.find(input, i, Math.min(MAX_LEN, input.length - i));
			if (s != null) {
				// symbols are represented by a code, which has 0 as higher bit, as they go from 0 to 127
				out.write(s.getCode());
				i += s.getKey().length();
			} else {
				// else plain byte
				out.write(ENTRY_ESCAPE);
				out.write(input[i]);
				i++;
			}
		}
		return out.toByteArray();
	}
}