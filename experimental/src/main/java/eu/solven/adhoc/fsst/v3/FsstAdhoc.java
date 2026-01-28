package eu.solven.adhoc.fsst.v3;

import org.maplibre.mlt.converter.encodings.fsst.Fsst;
import org.maplibre.mlt.converter.encodings.fsst.SymbolTable;

import eu.solven.adhoc.fsst.v3.SymbolTable.ByteSlice;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;

public class FsstAdhoc implements Fsst {

	@Override
	public SymbolTable encode(byte[] data) {
		var st = FsstTrain.train(new byte[][] { data });

		ByteSlice encoded = st.encodeAll(data);

		ByteArrayList concatenatedSymbols = new ByteArrayList();
		IntArrayList symbolsLength = new IntArrayList();

		for (int i = 0; i < st.decLen.length; i++) {
			int symLen = Byte.toUnsignedInt(st.decLen[i]);

			long symVal = st.decSymbol[i];

			for (int ii = 0; ii < symLen; ii++) {
				concatenatedSymbols.add((byte) ((symVal >> (8 * ii)) & 0xFF));
			}

			symbolsLength.add(symLen);
		}

		return new SymbolTable(null, null, encoded.asByteArray(), data.length);
	}

	// @Override
	// public byte[] decode(
	// byte[] symbols, int[] symbolLengths, byte[] compressedData, int decompressedLength) {
	//
	// }

}
