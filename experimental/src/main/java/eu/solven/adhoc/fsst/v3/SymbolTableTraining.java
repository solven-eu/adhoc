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
			// collision in hash table
			return false;
		}
		// TODO Why changing the value?
		long mask = ~0L >> sym.ignoredBits();
		hashTab[idx] = new Symbol(sym.val & mask, sym.icl);
		return true;
	}

	/**
	 * Add a symbol. Might fail in case of hash conflict.
	 * 
	 * @param sym
	 * @return
	 */
	public boolean addSymbol(Symbol sym) {
		int newCode = fsstCodeBase + nSymbols;
		if (newCode >= fsstCodeMax) {
			return false;
		}

		sym = sym.withCode(newCode);

		int length = sym.length();
		switch (length) {
		case 1 -> byteCodes[sym.first()] = packCodeLength(newCode, 1);
		case 2 -> shortCodes[sym.first2()] = packCodeLength(newCode, 2);
		default -> {
			if (!hashInsert(sym)) {
				return false;
			}
		}
		}
		symbols[newCode] = sym;
		nSymbols++;
		lenHisto[length - 1]++; // -1 as there is no code with length==0
		return true;
	}

	// findLongestSymbol returns the code for the longest matching symbol.
	public int findLongestSymbol(Symbol sym) {
		int idx = sym.hash();
		Symbol entry = hashTab[idx];
		if (entry.icl <= sym.icl) {
			long mask = ~0L >>> entry.ignoredBits();
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

	// rationale for finalize:
	// - during symbol table construction, we may create more than 256 codes, but bring it down to max 255 in the last
	// makeTable()
	// consequently we needed more than 8 bits during symbol table contruction, but can simplify the codes to single
	// bytes in finalize()
	// (this feature is in fact lo longer used, but could still be exploited: symbol construction creates no more than
	// 255 symbols in each pass)
	// - we not only reduce the amount of codes to <255, but also *reorder* the symbols and renumber their codes, for
	// higher compression perf.
	// we renumber codes so they are grouped by length, to allow optimized scalar string compression (byteLim and
	// suffixLim optimizations).
	// - we make the use of byteCode[] no longer necessary by inserting single-byte codes in the free spots of
	// shortCodes[]
	// Using shortCodes[] only makes compression faster. When creating the symbolTable, however, using shortCodes[] for
	// the single-byte
	// symbols is slow, as each insert touches 256 positions in it. This optimization was added when optimizing
	// symbolTable construction time.
	//
	// In all, we change the layout and coding, as follows..
	//
	// before finalize():
	// - The real symbols are symbols[256..256+nSymbols>. As we may have nSymbols > 255
	// - The first 256 codes are pseudo symbols (all escaped bytes)
	//
	// after finalize():
	// - table layout is symbols[0..nSymbols>, with nSymbols < 256.
	// - Real codes are [0,nSymbols>. 8-th bit not set.
	// - Escapes in shortCodes have the 8th bit set (value: 256+255=511). 255 because the code to be emitted is the
	// escape byte 255
	// - symbols are grouped by length: 2,3,4,5,6,7,8, then 1 (single-byte codes last)
	// the two-byte codes are split in two sections:
	// - first section contains codes for symbols for which there is no longer symbol (no suffix). It allows an
	// early-out during compression
	//
	// finally, shortCodes[] is modified to also encode all single-byte symbols (hence byteCodes[] is not required on a
	// critical path anymore).
	//
	public SymbolTable finalizeTable() {
		assert nSymbols <= 255;

		// codes are remapped
		int[] newCode = new int[256];

		// compute running sum of code lengths (starting offsets for each length)
		int[] runningSum = new int[8];
		int byteLim = nSymbols - lenHisto[0];
		runningSum[0] = byteLim;// 1-byte codes are highest
		runningSum[1] = 0; // zeroTerminated is not ported
		for (int i = 1; i < 7; i++) {
			runningSum[i + 1] = runningSum[i] + lenHisto[i];
		}

		int suffixLim = reorderCodes(newCode, runningSum);

		// TODO rebuildIndices should be a bit faster
		buildIndices();

		// rebuildIndices(newCode);
		SymbolTableDecoding decoding = buildDecoderTables(suffixLim);
		return new SymbolTable(this, decoding);
	}

	@SuppressWarnings({ "PMD.UseVarargs", "PMD.AssignmentInOperand" })
	private int reorderCodes(int[] newCode, int[] runningSum) {
		// determine the new code for each symbol, ordered by length (and splitting 2byte symbols into two classes
		// around suffixLim)
		int suffixLim = runningSum[1];

		int conflictCode = runningSum[2];
		for (int i = 0; i < nSymbols; i++) {
			Symbol sym1 = symbols[fsstCodeBase + i];
			int length = sym1.length();
			if (length == 2) {
				boolean hasConflict = false;
				int first2 = sym1.first2();
				for (int k = 0; k < nSymbols; k++) {
					if (k != i) {
						Symbol sym2 = symbols[fsstCodeBase + k];
						// test if symbol2 is a suffix of symbol1
						if (sym2.length() > 1 && sym2.first2() == first2) {
							hasConflict = true;
							break;
						}
					}
				}
				// symbols without a larger suffix have a code < suffixLim
				if (hasConflict) {
					newCode[i] = --conflictCode;
				} else {
					newCode[i] = suffixLim++;
				}
			} else {
				// length==1 or length >= 3
				newCode[i] = runningSum[length - 1];
				runningSum[length - 1]++;
			}
			// if (i != newCode[i]) {
			// Write from `[256, 512]` to `[0, 255]`
			symbols[newCode[i]] = sym1.withCode(newCode[i]);
			// }
		}
		return suffixLim;
	}

	void buildIndices() {
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

		for (int i = nSymbols - 1; i >= 0; i--) {
			// Should iterate from longest symbol to smaller ones, in order to fill hash with longest in priority
			Symbol sym = symbols[i];
			if (sym.length() == 2) {
				shortCodes[sym.first2()] = packCodeLength(i, 2);
			} else if (sym.length() >= 3) {
				hashInsert(sym);
			}
		}
	}

	@SuppressWarnings("PMD.UseVarargs")
	void rebuildIndices(int[] newCodes) {
		// renumber the codes in byteCodes[]
		{
			int defaultBytecode = packCodeLength(fsstCodeMask, 1);
			for (int i = 0; i < nSymbols; i++) {
				Symbol sym = symbols[i];
				int newCode;
				if (sym.length() == 1) {
					newCode = packCodeLength(i, 1);
				} else {
					newCode = defaultBytecode;
				}
				byteCodes[sym.first()] = newCode;
			}
		}

		// renumber the codes in shortCodes[]
		for (int i = 0; i < shortCodes.length; i++) {
			if ((shortCodes[i] & fsstCodeMask) >= fsstCodeBase) {
				// There is already an encoded shortCode: map the new encoded shortCode
				// Existing code may be length 1 or 2
				// BEWARE How could it be length==1?
				// BEWARE Small optimization by short-circuiting packCodeLength
				shortCodes[i] = newCodes[shortCodes[i] & 0xFF] + (shortCodes[i] & (15 << fsstLenBits));
			} else {
				shortCodes[i] = byteCodes[i & fsstMask8];
			}
		}

		// replace the symbols in the hash table
		// Symbol empty = new Symbol(0, fsstICLFree);
		// Arrays.fill(hashTab, empty);
		// for (int i = 0; i < nSymbols; i++) {
		// Symbol sym = symbols[i];
		// if (sym.length() == 2) {
		// shortCodes[sym.first2()] = packCodeLength(i, 2);
		// } else if (sym.length() >= 3) {
		// boolean inserted = hashInsert(sym);
		// assert inserted == true;
		// }
		// }

		for (int i = 0; i < fsstHashTabSize; i++) {
			if (hashTab[i].icl < fsstICLFree) {
				// TODO How are we guaranteed to pick the longest code with given hash?
				hashTab[i] = symbols[newCodes[hashTab[i].first()]];
			}
			// else {
			// the hashTab entry is free, and remains free with new code mapping
			// }
		}
	}

	SymbolTableDecoding buildDecoderTables(int suffixLim) {
		// Decoder tables
		byte[] decLen = new byte[255]; // code -> symbol length
		long[] decSymbol = new long[255]; // code -> symbol value

		for (int code = 0; code < nSymbols; code++) {
			Symbol sym = symbols[code];
			decLen[code] = (byte) sym.length();
			decSymbol[code] = sym.val;
		}

		return new SymbolTableDecoding(decLen, decSymbol, suffixLim);
	}
}