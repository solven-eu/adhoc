package eu.solven.adhoc.fsst.v3;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import javax.annotation.concurrent.NotThreadSafe;

import eu.solven.adhoc.fsst.v3.SymbolUtil.Symbol;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;

/**
 * Represents a table of symbols, enabling encoding and decoding.
 * 
 * @author Benoit Lacelle
 */
// Not thread-safe due to shared encBuf
@NotThreadSafe
@RequiredArgsConstructor
public class SymbolTable2 implements IFsstConstants, Externalizable {
	final SymbolTable symbolTable;

	// Decoder tables
	private final byte[] decLen; // code -> symbol length
	private final long[] decSymbol; // code -> symbol value

	private final int suffixLim;

	@AllArgsConstructor
	public static final class ByteSlice {
		final byte[] array;
		final int offset;
		final int length;
	}

	// Encode compresses input, reusing buf if provided.
	// Returns compressed data (may be a different slice than buf).
	public ByteSlice encode(byte[] buf, byte[] input) {
		if (buf == null || buf.length < 2 * input.length + fsstOutputPadding)
			buf = new byte[2 * input.length + fsstOutputPadding];
		int outPos = 0, pos = 0;
		int inputLen = input.length;

		int tailLen = inputLen % 8;

		// Process with safe unaligned loads while >=8 bytes remain
		while (pos + 8 <= inputLen) {
			int chunkEnd = Math.min(pos + fsstChunkSize, inputLen - tailLen);
			outPos = encodeChunk(buf, outPos, input, pos, chunkEnd - pos);
			pos = chunkEnd;
		}

		// Handle tail with padded buffer
		if (pos < inputLen) {
			// makes a new buffer with padded 0
			// it enables fsstUnalignedLoad assuming the input always have expected size
			byte[] encBuf = Arrays.copyOfRange(input, pos, pos + 8);
			outPos = encodeChunk(buf, outPos, encBuf, 0, tailLen);
		}
		return new ByteSlice(buf, 0, outPos);
	}

	public ByteSlice encodeAll(byte[] input) {
		return encode(null, input);
	}

	/**
	 * 
	 * @param dst
	 *            the encoded output
	 * @param dstPos
	 *            the initial position at which to write encoded bytes
	 * @param input
	 *            the decoded input
	 * @param pos
	 *            the start position for eading input
	 * @param end
	 *            may be shorter than input (e.g. if we restrict to a chunk, or if we padded with 0)
	 * @return
	 */
	// encodeChunk compresses buf[0:end] to dst starting at dstPos.
	// buf must have >=8 bytes padding after end for safe unaligned loads.
	private int encodeChunk(byte[] dst, int dstPos, byte[] input, int pos, int end) {
		int suffixLimit = suffixLim;

		while (pos < end) {
			long word = SymbolUtil.fsstUnalignedLoad(input, pos);

			// Try 2-byte fast path (unique prefix)
			int code = symbolTable.shortCodes[(int) (word & fsstMask16)];
			if ((code & 0xFF) < suffixLimit && pos + 2 <= end) {
				dst[dstPos++] = (byte) code;
				pos += 2;
				continue;
			}

			// Try 3-8 byte hash table match
			int idx = (int) (SymbolUtil.fsstHash(word & fsstMask24) & (fsstHashTabSize - 1));
			Symbol entry = symbolTable.hashTab[idx];
			if (entry.icl < fsstICLFree) {
				long mask = ~0L >> entry.ignoredBits();
				int symLen = entry.length();
				if ((entry.val & mask) == (word & mask) && pos + symLen <= end) {
					dst[dstPos++] = (byte) entry.code();
					pos += symLen;
					continue;
				}
			}

			// Fall back to 2-byte (if valid)
			int advance = code >> fsstLenBits;
			if (pos + advance > end) {
				// or 1-byte/escape (if input too short)
				code = symbolTable.byteCodes[input[pos] & 0xFF];
				advance = 1;
			}

			dst[dstPos++] = (byte) (code & fsstMask8);
			if ((code & fsstCodeBase) != 0)
				dst[dstPos++] = input[pos];
			pos += advance;
		}
		return dstPos;
	}

	public ByteSlice encodeAll(String string) {
		return encodeAll(string.getBytes(StandardCharsets.UTF_8));
	}

	private ByteSlice decode(byte[] buf, ByteSlice src) {
		return decode(buf, src.array, src.offset, src.offset + src.length);
	}

	public ByteSlice decode(byte[] buf, byte[] src) {
		return decode(buf, src, 0, src.length);
	}

	public ByteSlice decode(byte[] buf, byte[] src, int srcStart, int srcEnd) {
		if (buf == null)
			buf = new byte[src.length * 4 + 8];
		int bufPos = 0, srcPos = srcStart;
		int bufCap = buf.length;

		if (srcEnd >= src.length) {
			throw new IllegalArgumentException();
		}

		while (srcPos < srcEnd) {
			int code = src[srcPos++] & 0xFF;
			if (code < fsstEscapeCode) {
				int symLen = decLen[code];
				long symVal = decSymbol[code];

				if (bufPos + symLen > bufCap) {
					// extends the buffer as it it too small to accept decoded bytes
					int newCap = Math.max(bufCap * 2, bufPos + symLen);
					buf = Arrays.copyOf(buf, newCap);
					bufCap = newCap;
				}

				for (int i = 0; i < symLen; i++) {
					buf[bufPos + i] = (byte) ((symVal >> (8 * i)) & 0xFF);
				}
				bufPos += symLen;
			} else {
				if (srcPos >= srcEnd)
					break;
				if (bufPos >= bufCap)
					buf = Arrays.copyOf(buf, Math.max(bufCap * 2, bufPos + 1));
				buf[bufPos++] = src[srcPos++];
			}
		}
		return new ByteSlice(buf, 0, bufPos);
	}

	public ByteSlice decodeAll(byte[] src) {
		return decode(null, src);
	}

	public ByteSlice decodeAll(ByteSlice src) {
		return decode(null, src);
	}

	@Override
	public void writeExternal(ObjectOutput w) throws IOException {
		long ver = (FSST_VERSION << 32) | ((long) suffixLim << 16) | ((long) symbolTable.nSymbols << 8) | 1;
		long n = 0;
		byte[] buf8 = new byte[8];
		ByteBuffer.wrap(buf8).order(ByteOrder.LITTLE_ENDIAN).putLong(ver);
		w.write(buf8);
		n += buf8.length;

		// lenHisto
		byte[] lh = new byte[8];
		int[] hist = new int[8];
		for (int i = 0; i < symbolTable.nSymbols; i++) {
			int len = symbolTable.symbols[i].length();
			if (len >= 1 && len <= 8)
				hist[len - 1]++;
		}
		for (int i = 0; i < 8; i++)
			lh[i] = (byte) hist[i];
		w.write(lh);
		n += lh.length;

		// symbol bytes
		for (int i = 0; i < symbolTable.nSymbols; i++) {
			Symbol sym = symbolTable.symbols[i];
			int len = sym.length();
			Arrays.fill(buf8, (byte) 0);
			for (int b = 0; b < len; b++)
				buf8[b] = (byte) ((sym.val >> (8 * b)) & 0xFF);
			w.write(buf8, 0, len);
			n += len;
		}
		// return n;
	}

	@Override
	public void readExternal(ObjectInput r) throws IOException, ClassNotFoundException {
		// SymbolTable t = SymbolTable.makeSymbolTable(); // reset
		SymbolTable t = symbolTable;

		byte[] hdr = new byte[8];
		long n = 0;
		if (r.read(hdr, 0, 8) != 8)
			throw new EOFException();
		n += 8;
		long ver = ByteBuffer.wrap(hdr).order(ByteOrder.LITTLE_ENDIAN).getLong();
		if ((ver >> 32) != FSST_VERSION)
			throw new IOException("fsst: unsupported table version");
		int suffixLim = (int) ((ver >> 16) & fsstMask8);
		t.nSymbols = (int) ((ver >> 8) & fsstMask8);

		byte[] lh = new byte[8];
		if (r.read(lh, 0, 8) != 8)
			throw new EOFException();

		n += 8;
		for (int i = 0; i < 8; i++)
			t.lenHisto[i] = lh[i] & 0xFF;

		// Build length schedule
		int[] lens = new int[t.nSymbols];
		int pos = 0;
		for (int l = 2; l <= 8; l++) {
			for (int j = 0; j < t.lenHisto[l - 1]; j++)
				lens[pos++] = l;
		}
		for (int j = 0; j < t.lenHisto[0]; j++)
			lens[pos++] = 1;

		byte[] b8 = new byte[8];
		for (int i = 0; i < t.nSymbols; i++) {
			int len = lens[i];
			if (r.read(b8, 0, len) != len)
				throw new EOFException();
			n += len;
			long val = 0;
			for (int b = 0; b < len; b++)
				val |= ((long) b8[b] & 0xFF) << (8 * b);
			Symbol sym = new Symbol(val, Symbol.evalICL(i, len));
			t.symbols[i] = sym;
		}

		t.rebuildIndices();
		SymbolTable2 decoded = t.buildDecoderTables(suffixLim);

		System.arraycopy(decoded.decLen, 0, this.decLen, 0, this.decLen.length);
		System.arraycopy(decoded.decSymbol, 0, this.decSymbol, 0, this.decSymbol.length);
	}

}