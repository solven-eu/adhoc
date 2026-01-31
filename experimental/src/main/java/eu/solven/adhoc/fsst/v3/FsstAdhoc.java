package eu.solven.adhoc.fsst.v3;

import org.maplibre.mlt.converter.encodings.fsst.Fsst;
import org.maplibre.mlt.converter.encodings.fsst.SymbolTable;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;

public class FsstAdhoc implements Fsst {

	public eu.solven.adhoc.fsst.v3.SymbolTable train(byte[] data) {
		var st = FsstTrain.train(new byte[][] { data });
		return st;
	}

	public SymbolTable encode(byte[] data, eu.solven.adhoc.fsst.v3.SymbolTable st2) {
		SymbolTableDecoding st = st2.decoding;
		ByteSlice encoded = st2.encodeAll(data);

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

		return new SymbolTable(concatenatedSymbols.elements(),
				symbolsLength.elements(),
				encoded.asByteArray(),
				data.length);
	}

	@Override
	public SymbolTable encode(byte[] data) {
		var st = train(data);

		return encode(data, st);
	}

}
