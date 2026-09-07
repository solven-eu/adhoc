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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.encoding.fsst.SymbolUtil.Symbol;

public class TestSymbolTableTraining {

	@Test
	public void testMakeSymbolTable_isEmpty() {
		SymbolTableTraining st = SymbolTableTraining.makeSymbolTable();
		Assertions.assertThat(st.getNSymbols()).isZero();
		Assertions.assertThat(st.toString()).isEmpty();
	}

	@Test
	public void testAddSymbol_singleByte() {
		SymbolTableTraining st = SymbolTableTraining.makeSymbolTable();
		// length=1 branch in addSymbol's switch
		Symbol sym = new Symbol(0x41L, Symbol.evalICL(IFsstConstants.fsstCodeMax, 1));
		boolean added = st.addSymbol(sym);
		Assertions.assertThat(added).isTrue();
		Assertions.assertThat(st.getNSymbols()).isEqualTo(1);
	}

	@Test
	public void testAddSymbol_twoBytes() {
		SymbolTableTraining st = SymbolTableTraining.makeSymbolTable();
		// length=2 branch in addSymbol's switch
		Symbol sym = new Symbol(0x4241L, Symbol.evalICL(IFsstConstants.fsstCodeMax, 2));
		boolean added = st.addSymbol(sym);
		Assertions.assertThat(added).isTrue();
		Assertions.assertThat(st.getNSymbols()).isEqualTo(1);
	}

	@Test
	public void testAddSymbol_threeBytes_throughHash() {
		SymbolTableTraining st = SymbolTableTraining.makeSymbolTable();
		// length>=3 branch: goes through hashInsert
		Symbol sym = new Symbol(0x434241L, Symbol.evalICL(IFsstConstants.fsstCodeMax, 3));
		boolean added = st.addSymbol(sym);
		Assertions.assertThat(added).isTrue();
		Assertions.assertThat(st.getNSymbols()).isEqualTo(1);
	}

	@Test
	public void testToString_withSymbol() {
		SymbolTableTraining st = SymbolTableTraining.makeSymbolTable();
		Symbol sym = new Symbol(0x4241L, Symbol.evalICL(IFsstConstants.fsstCodeMax, 2));
		st.addSymbol(sym);
		// The 2-byte symbol is listed by toString
		Assertions.assertThat(st.toString()).contains("l=2").contains("c=256");
	}

	@Test
	public void testFindLongestSymbol_singleByteDefault() {
		SymbolTableTraining st = SymbolTableTraining.makeSymbolTable();
		// No symbols added: the byteCodes default maps 0x41 to code 0x41
		Symbol query = Symbol.fromBytes(new byte[] { 0x41, 0x42, 0x43 }, 0);
		int code = st.findLongestSymbol(query);
		Assertions.assertThat(code).isEqualTo(0x41);
	}

	@Test
	public void testFindLongestSymbol_twoByteCode() {
		SymbolTableTraining st = SymbolTableTraining.makeSymbolTable();
		// Add "AB" as a 2-byte symbol → shortCodes[0x4241] updated
		Symbol sym = new Symbol(0x4241L, Symbol.evalICL(IFsstConstants.fsstCodeMax, 2));
		st.addSymbol(sym);
		// Query starting with 0x41,0x42 should return the newly assigned code (fsstCodeBase)
		Symbol query = Symbol.fromBytes(new byte[] { 0x41, 0x42, 0x43 }, 0);
		int code = st.findLongestSymbol(query);
		Assertions.assertThat(code).isEqualTo(IFsstConstants.fsstCodeBase);
	}

	@Test
	public void testFindLongestSymbol_threeByteHashMatch() {
		SymbolTableTraining st = SymbolTableTraining.makeSymbolTable();
		// Add a 3-byte symbol via hash table
		Symbol sym = new Symbol(0x010203L, Symbol.evalICL(IFsstConstants.fsstCodeMax, 3));
		st.addSymbol(sym);
		// Query that starts with the same 3 bytes → hash match
		Symbol query = Symbol.fromBytes(new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 }, 0);
		int code = st.findLongestSymbol(query);
		Assertions.assertThat(code).isEqualTo(IFsstConstants.fsstCodeBase);
	}

	@Test
	public void testHashInsert_collision() {
		SymbolTableTraining st = SymbolTableTraining.makeSymbolTable();
		boolean collisionSeen = false;
		// With 2048 hash slots and many distinct 3-byte values, collisions are inevitable
		for (long v = 1; v <= 0x10000L && !collisionSeen; v++) {
			Symbol sym = new Symbol(v & 0xFFFFFFL, Symbol.evalICL(IFsstConstants.fsstCodeMax, 3));
			boolean inserted = st.hashInsert(sym);
			if (!inserted) {
				collisionSeen = true;
			}
		}
		Assertions.assertThat(collisionSeen).isTrue();
	}

	@Test
	public void testAddSymbol_hashCollision_returnsFalse() {
		SymbolTableTraining st = SymbolTableTraining.makeSymbolTable();
		boolean failedDueToCollision = false;
		// Iterate until addSymbol fails (hash collision before code-space exhaustion)
		for (long v = 1; v <= 0x10000L && !failedDueToCollision; v++) {
			Symbol sym = new Symbol(v & 0xFFFFFFL, Symbol.evalICL(IFsstConstants.fsstCodeMax, 3));
			boolean ok = st.addSymbol(sym);
			if (!ok) {
				failedDueToCollision = true;
			}
		}
		Assertions.assertThat(failedDueToCollision).isTrue();
	}

	@Test
	public void testFinalizeTable_mixedLengths() {
		SymbolTableTraining st = SymbolTableTraining.makeSymbolTable();
		// 1-byte symbol
		st.addSymbol(new Symbol(0x41L, Symbol.evalICL(IFsstConstants.fsstCodeMax, 1)));
		// 2-byte symbol "AB" – no conflict (first2 = 0x4241)
		st.addSymbol(new Symbol(0x4241L, Symbol.evalICL(IFsstConstants.fsstCodeMax, 2)));
		// 3-byte symbol "AB!" – shares first2 with "AB" → hasConflict=true for "AB" in reorderCodes
		st.addSymbol(new Symbol(0x214241L, Symbol.evalICL(IFsstConstants.fsstCodeMax, 3)));
		// 2-byte symbol with different first2 → hasConflict=false
		st.addSymbol(new Symbol(0x4443L, Symbol.evalICL(IFsstConstants.fsstCodeMax, 2)));

		// finalizeTable exercises reorderCodes, buildIndices, buildDecoderTables
		SymbolTable table = st.finalizeTable();
		Assertions.assertThat(table).isNotNull();
	}
}
