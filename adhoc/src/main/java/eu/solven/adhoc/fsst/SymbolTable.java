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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Holds the symbol table.
 * 
 * @author Benoit Lacelle
 * 
 */
public final class SymbolTable {
	// enable fast lookup as long as encoding are needed
	final Map<ByteArrayKey, Symbol> lookup = new HashMap<>();
	// refer symbol by code for decoding
	final List<Symbol> byCode = new ArrayList<>();
	final List<Boolean> usedForEncoding = new ArrayList<>();

	public void addSymbol(ByteArrayKey key, int code) {
		Symbol s = new Symbol(key, code);
		lookup.put(key, s);
		byCode.add(s);
		usedForEncoding.add(false);
	}

	/**
	 * Given an input at given offset, we look for the largest symbol matching the next bytes.
	 * 
	 * @param input
	 * @param offset
	 * @param maxLen
	 * @return
	 */
	public Symbol find(byte[] input, int offset, int maxLen) {
		for (int len = maxLen; len > 0; len--) {

			ByteArrayKey key = ByteArrayKey.read(input, offset, len);
			Symbol s = lookup.get(key);
			if (s != null) {
				usedForEncoding.set(s.getCode(), true);
				return s;
			}
		}
		return null;
	}

	public Symbol getByCode(int code) {
		return byCode.get(code);
	}

	public int size() {
		return byCode.size();
	}

	public void freeze() {
		// Once frozen, we do not encode any new entry: the lookup table can be dropped.
		lookup.clear();

		for (int i = size(); i >= 0; i--) {
			// Check if the symbol is actually used
			if (!usedForEncoding.get(i)) {
				// If not, we can remove the symbol
				byCode.set(i, null);
			}
		}
	}
}