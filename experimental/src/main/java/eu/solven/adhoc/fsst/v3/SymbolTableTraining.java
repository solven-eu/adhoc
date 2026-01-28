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
package eu.solven.adhoc.fsst.v3;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.concurrent.NotThreadSafe;

import eu.solven.adhoc.fsst.v3.SymbolUtil.Symbol;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Represents a table of symbols for training purposes.
 * 
 * @author Benoit Lacelle
 */
// Not thread-safe due to shared encBuf
@NotThreadSafe
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@SuppressWarnings("checkstyle:MagicNumber")
public final class SymbolTableTraining implements IFsstConstants {

	final int[] shortCodes;
	final int[] byteCodes;
	final Symbol[] symbols;
	final Symbol[] hashTab;

	private static final SymbolTableTraining EMPTY = new SymbolTableTraining();

	// Symbol metadata
	@Getter
	int nSymbols = 0;
	final int[] lenHisto = new int[8]; // histogram of lengths 1-8

	/**
	 * @return a fresh and empty {@link SymbolTableTraining}
	 */
	public static SymbolTableTraining makeSymbolTable() {
		// Clone an empty symboltable
		return new SymbolTableTraining(EMPTY.shortCodes.clone(),
				EMPTY.byteCodes.clone(),
				EMPTY.symbols.clone(),
				EMPTY.hashTab.clone());
	}

	/**
	 * Make an {@link SymbolTableTraining}. This is not a free operation.
	 */
	private SymbolTableTraining() {
		shortCodes = new int[65_536]; // 2-byte prefix -> packed [length|code]
		byteCodes = new int[256]; // 1-byte -> packed [length|code]
		symbols = new Symbol[fsstCodeMax]; // code -> symbol (for decoding and training)
		hashTab = new Symbol[fsstHashTabSize]; // direct-mapped 3-8 byte symbols

		// Codes 0-255 are escape codes
		for (int i = 0; i < 256; i++) {
			symbols[i] = Symbol.newSymbolFromByte((byte) i, i);
		}
		Symbol unused = new Symbol(0, fsstICLFree);
		Arrays.fill(symbols, 256, fsstCodeMax, unused);

		Symbol empty = new Symbol(0, fsstICLFree);
		Arrays.fill(hashTab, empty);

		// by default, we write codes in 0-255
		// they might later be upgraded into symbol (i.e. with code >=256)
		for (int i = 0; i < 256; i++) {
			byteCodes[i] = packCodeLength(i, 1);
		}
		for (int i = 0; i < 65_536; i++) {
			shortCodes[i] = packCodeLength(i & fsstMask8, 1);
		}
	}

	@Override
	public String toString() {
		return Stream.of(symbols)
				// skip the 1-byte symbols which are hardcoded
				.skip(256)
				.filter(s -> s.icl != fsstICLFree)
				.map(s -> "l=%s c=%s value=%s".formatted(s.length(), s.code(), s.val))
				.collect(Collectors.joining(", "));
	}

	private static int packCodeLength(int code, int length) {
		return (length << fsstLenBits) | (code & fsstCodeMask);
	}

	public boolean hashInsert(Symbol sym) {
		int idx = sym.hash();
		if (hashTab[idx].icl < fsstICLFree) {
			return false;
		}
		long mask = ~0L >> sym.ignoredBits();
		hashTab[idx] = new Symbol(sym.val & mask, sym.icl);
		return true;
	}

	public boolean addSymbol(Symbol sym) {
		if (fsstCodeBase + nSymbols >= fsstCodeMax) {
			return false;
		}

		sym = sym.withCode(fsstCodeBase + nSymbols);

		int length = sym.length();
		switch (length) {
		case 1 -> byteCodes[sym.first()] = packCodeLength(fsstCodeBase + nSymbols, 1);
		case 2 -> shortCodes[sym.first2()] = packCodeLength(fsstCodeBase + nSymbols, 2);
		default -> {
			if (!hashInsert(sym)) {
				return false;
			}
		}
		}
		symbols[fsstCodeBase + nSymbols] = sym;
		nSymbols++;
		lenHisto[length - 1]++;
		return true;
	}

	// findLongestSymbol returns the code for the longest matching symbol.
	public int findLongestSymbol(Symbol sym) {
		int idx = sym.hash();
		Symbol entry = hashTab[idx];
		if (entry.icl <= sym.icl) {
			long mask = ~0L >> entry.ignoredBits();
			if (entry.val == (sym.val & mask)) {
				return entry.code() & fsstCodeMask;
			}
		}
		if (sym.length() >= 2) {
			int code = shortCodes[sym.first2()] & fsstCodeMask;
			if (code >= fsstCodeBase) {
				return code;
			}
		}
		return byteCodes[sym.first()] & fsstCodeMask;
	}

	@SuppressWarnings("PMD.AssignmentInOperand")
	public SymbolTable finalizeTable() {
		int[] newCode = new int[256];
		int[] codeStart = new int[8];
		int byteLim = nSymbols - lenHisto[0];
		codeStart[0] = byteLim;
		codeStart[1] = 0;
		for (int i = 1; i < 7; i++) {
			codeStart[i + 1] = codeStart[i] + lenHisto[i];
		}
		int suffixLim = codeStart[1];

		// Partition 2-byte symbols
		int conflictCode = codeStart[2];
		for (int i = 0; i < nSymbols; i++) {
			Symbol sym = symbols[fsstCodeBase + i];
			int length = sym.length();
			if (length == 2) {
				boolean hasConflict = false;
				int first2 = sym.first2();
				for (int k = 0; k < nSymbols; k++) {
					if (k != i) {
						Symbol other = symbols[fsstCodeBase + k];
						if (other.length() > 1 && other.first2() == first2) {
							hasConflict = true;
							break;
						}
					}
				}
				if (hasConflict) {
					newCode[i] = --conflictCode;
				} else {
					newCode[i] = suffixLim++;
				}
			} else {
				newCode[i] = codeStart[length - 1];
				codeStart[length - 1]++;
			}
			symbols[newCode[i]] = sym.withCode(newCode[i]);
		}
		rebuildIndices();
		return buildDecoderTables(suffixLim);
	}

	void rebuildIndices() {
		Arrays.fill(byteCodes, packCodeLength(fsstCodeMask, 1));
		Symbol empty = new Symbol(0, fsstICLFree);
		Arrays.fill(hashTab, empty);

		for (int i = 0; i < nSymbols; i++) {
			Symbol sym = symbols[i];
			if (sym.length() == 1) {
				byteCodes[sym.first()] = packCodeLength(i, 1);
			}
		}

		for (int i = 0; i < shortCodes.length; i++) {
			shortCodes[i] = byteCodes[i & fsstMask8];
		}

		for (int i = 0; i < nSymbols; i++) {
			Symbol sym = symbols[i];
			if (sym.length() == 2) {
				shortCodes[sym.first2()] = packCodeLength(i, 2);
			}
			if (sym.length() >= 3) {
				hashInsert(sym);
			}
		}
	}

	SymbolTable buildDecoderTables(int suffixLim) {
		// Decoder tables
		byte[] decLen = new byte[255]; // code -> symbol length
		long[] decSymbol = new long[255]; // code -> symbol value

		for (int code = 0; code < nSymbols; code++) {
			Symbol sym = symbols[code];
			decLen[code] = (byte) sym.length();
			decSymbol[code] = sym.val;
		}

		return new SymbolTable(this, decLen, decSymbol, suffixLim);
	}
}