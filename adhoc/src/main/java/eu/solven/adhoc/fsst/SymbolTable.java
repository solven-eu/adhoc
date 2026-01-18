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
	protected final Map<ByteArrayKey, Symbol> lookup = new HashMap<>();
	// refer symbol by code for decoding
	protected final List<Symbol> byCode = new ArrayList<>();
	protected final List<Boolean> usedForEncoding = new ArrayList<>();

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