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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.encoding.bytes.IByteSlice;

public class TestSymbolTableExternalizable {

	/** Serializes the given {@link SymbolTable} and deserializes it back. */
	private SymbolTable roundTrip(SymbolTable original) throws IOException, ClassNotFoundException {
		SymbolTableExternalizable wrapper = SymbolTableExternalizable.wrap(original);

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
			oos.writeObject(wrapper);
		}

		try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
			SymbolTableExternalizable loaded = (SymbolTableExternalizable) ois.readObject();
			return loaded.symbolTable;
		}
	}

	@Test
	public void testRoundTrip_helloWorld() throws IOException, ClassNotFoundException {
		SymbolTable original = (SymbolTable) FsstTrainer.builder().build().train("hello world hello world hello world");

		SymbolTable restored = roundTrip(original);

		String input = "hello world";
		IByteSlice encoded = original.encodeAll(input);
		IByteSlice decoded = restored.decodeAll(encoded);
		Assertions.assertThat(decoded.asString(StandardCharsets.UTF_8)).isEqualTo(input);
	}

	@Test
	public void testRoundTrip_longerSymbols() throws IOException, ClassNotFoundException {
		// Longer corpus → trainer may produce multi-byte (3–8) symbols
		String corpus = "ABCDEFGH ABCDEFGH ABCDEFGH ABCDEFGH ABCDEFGH ABCDEFGH";
		SymbolTable original = (SymbolTable) FsstTrainer.builder().build().train(corpus);

		SymbolTable restored = roundTrip(original);

		String input = "ABCDEFGH";
		IByteSlice encoded = original.encodeAll(input);
		IByteSlice decoded = restored.decodeAll(encoded);
		Assertions.assertThat(decoded.asString(StandardCharsets.UTF_8)).isEqualTo(input);
	}

	@Test
	public void testRoundTrip_encoderAndDecoderBothWork() throws IOException, ClassNotFoundException {
		SymbolTable original =
				(SymbolTable) FsstTrainer.builder().build().train("the quick brown fox jumps over the lazy dog");

		SymbolTable restored = roundTrip(original);

		// Encode with restored and decode with original (cross-check)
		String input = "the quick brown fox";
		IByteSlice encodedWithRestored = restored.encodeAll(input);
		IByteSlice decodedWithOriginal = original.decodeAll(encodedWithRestored);
		Assertions.assertThat(decodedWithOriginal.asString(StandardCharsets.UTF_8)).isEqualTo(input);
	}
}
