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
package eu.solven.adhoc.encoding.fsst;

import org.maplibre.mlt.converter.encodings.fsst.Fsst;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;

/**
 * Adhoc implementation of MapLibre abstraction for FSST.
 * 
 * @author Benoit Lacelle
 */
@SuppressWarnings({ "PMD", "checkstyle:MagicNumber" })
@Deprecated
public class FsstAdhoc implements Fsst {

	public IFsstEncoding train(byte[] data) {
		var st = FsstTrainer.builder().build().train(new byte[][] { data });
		return st;
	}

	public org.maplibre.mlt.converter.encodings.fsst.SymbolTable encode(byte[] data, IFsstEncoding st2) {
		SymbolTableDecoding st = ((SymbolTable) st2).decoding;
		IByteSlice encoded = st2.encodeAll(data);

		ByteArrayList concatenatedSymbols = new ByteArrayList();
		IntArrayList symbolsLength = new IntArrayList(st.decLen.length);

		for (int i = 0; i < st.decLen.length; i++) {
			int symLen = Byte.toUnsignedInt(st.decLen[i]);

			if (symLen == 0) {
				break;
			}

			long symVal = st.decSymbol[i];

			for (int ii = 0; ii < symLen; ii++) {
				concatenatedSymbols.add((byte) ((symVal >> (8 * ii)) & 0xFF));
			}

			symbolsLength.add(symLen);
		}

		concatenatedSymbols.trim();
		symbolsLength.trim();

		return new org.maplibre.mlt.converter.encodings.fsst.SymbolTable(concatenatedSymbols.elements(),
				symbolsLength.elements(),
				encoded.cropped(),
				data.length);
	}

	@Override
	public org.maplibre.mlt.converter.encodings.fsst.SymbolTable encode(byte[] data) {
		var st = train(data);

		return encode(data, st);
	}

}
