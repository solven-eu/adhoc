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

import java.io.EOFException;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import eu.solven.adhoc.fsst.v3.SymbolUtil.Symbol;
import lombok.NoArgsConstructor;

/**
 * Enable serialization of a {@link SymbolTable}.
 * 
 * @author Benoit Lacelle
 * 
 */
// empty constructor for Externalizable
@SuppressWarnings({ "checkstyle:MagicNumber", "PMD.MissingSerialVersionUID" })
@NoArgsConstructor
public class SymbolTableExternalizable implements IFsstConstants, Externalizable {
	// not final for readExternal
	SymbolTable symbolTable;

	private SymbolTableExternalizable(SymbolTable symbolTable) {
		this.symbolTable = symbolTable;
	}

	public static SymbolTableExternalizable wrap(SymbolTable symbolTable) {
		return new SymbolTableExternalizable(symbolTable);
	}

	@Override
	public void writeExternal(ObjectOutput w) throws IOException {
		long ver = (FSST_VERSION << 32) | ((long) symbolTable.suffixLim << 16)
				| ((long) symbolTable.symbols.nSymbols << 8)
				| 1;
		byte[] buf8 = new byte[8];
		ByteBuffer.wrap(buf8).order(ByteOrder.LITTLE_ENDIAN).putLong(ver);
		w.write(buf8);

		// lenHisto
		byte[] lh = new byte[8];
		int[] hist = new int[8];
		for (int i = 0; i < symbolTable.symbols.nSymbols; i++) {
			int len = symbolTable.symbols.symbols[i].length();
			if (len >= 1 && len <= 8) {
				hist[len - 1]++;
			}
		}
		for (int i = 0; i < 8; i++) {
			lh[i] = (byte) hist[i];
		}
		w.write(lh);

		// symbol bytes
		for (int i = 0; i < symbolTable.symbols.nSymbols; i++) {
			Symbol sym = symbolTable.symbols.symbols[i];
			int len = sym.length();
			Arrays.fill(buf8, (byte) 0);
			for (int b = 0; b < len; b++) {
				buf8[b] = (byte) ((sym.val >> (8 * b)) & 0xFF);
			}
			w.write(buf8, 0, len);
		}
	}

	@SuppressWarnings("PMD.AssignmentInOperand")
	@Override
	public void readExternal(ObjectInput r) throws IOException, ClassNotFoundException {
		SymbolTableTraining t = SymbolTableTraining.makeSymbolTable();

		byte[] hdr = new byte[8];
		if (r.read(hdr, 0, 8) != 8) {
			throw new EOFException();
		}
		long ver = ByteBuffer.wrap(hdr).order(ByteOrder.LITTLE_ENDIAN).getLong();
		if ((ver >> 32) != FSST_VERSION) {
			throw new IOException("fsst: unsupported table version");
		}
		int suffixLim = (int) ((ver >> 16) & fsstMask8);
		t.nSymbols = (int) ((ver >> 8) & fsstMask8);

		byte[] lh = new byte[8];
		if (r.read(lh, 0, 8) != 8) {
			throw new EOFException();
		}

		for (int i = 0; i < 8; i++) {
			t.lenHisto[i] = lh[i] & 0xFF;
		}

		// Build length schedule
		int[] lens = new int[t.nSymbols];
		int pos = 0;
		for (int l = 2; l <= 8; l++) {
			for (int j = 0; j < t.lenHisto[l - 1]; j++) {
				lens[pos++] = l;
			}
		}
		for (int j = 0; j < t.lenHisto[0]; j++) {
			lens[pos++] = 1;
		}

		byte[] b8 = new byte[8];
		for (int i = 0; i < t.nSymbols; i++) {
			int len = lens[i];
			if (r.read(b8, 0, len) != len) {
				throw new EOFException();
			}
			long val = 0;
			for (int b = 0; b < len; b++) {
				val |= (b8[b] & 0xFF) << (8 * b);
			}
			Symbol sym = new Symbol(val, Symbol.evalICL(i, len));
			t.symbols[i] = sym;
		}

		t.rebuildIndices();
		SymbolTable decoded = t.buildDecoderTables(suffixLim);

		this.symbolTable = decoded;
		System.arraycopy(decoded.decLen, 0, this.symbolTable.decLen, 0, this.symbolTable.decLen.length);
		System.arraycopy(decoded.decSymbol, 0, this.symbolTable.decSymbol, 0, this.symbolTable.decSymbol.length);
	}

}
