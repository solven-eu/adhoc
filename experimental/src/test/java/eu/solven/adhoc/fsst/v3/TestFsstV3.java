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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.fsst.v3.SymbolTable.ByteSlice;
import eu.solven.adhoc.fsst.v3.SymbolUtil.Symbol;
import eu.solven.pepper.core.PepperLogHelper;
import eu.solven.pepper.io.PepperSerializationHelper;
import eu.solven.pepper.spring.PepperResourceHelper;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestFsstV3 {

	@Test
	public void setCodeLengthTest() {
		Symbol symbol = new Symbol(123L, Symbol.evalICL(50, 4));
		Assertions.assertThat(symbol.code()).isEqualTo(50);
		Assertions.assertThat(symbol.length()).isEqualTo(4);
		Assertions.assertThat(symbol.ignoredBits()).isEqualTo(32);
		Assertions.assertThat(symbol.val).isEqualTo(123L);
	}

	@Test
	public void testConcat() {
		Symbol symbol1 = Symbol.newSymbolFromBytes(new byte[] { 1, 2 });
		Symbol symbol2 = Symbol.newSymbolFromBytes(new byte[] { 3, 4, 5 });

		Symbol concat = SymbolUtil.fsstConcat(symbol1, symbol2);
		Assertions.assertThat(concat.code()).isEqualTo(511);
		Assertions.assertThat(concat.length()).isEqualTo(2 + 3);
		Assertions.assertThat(concat.ignoredBits()).isEqualTo(64 - (2 + 3) * 8);
		Assertions.assertThat(concat.val).isEqualTo(21542142465L);
	}

	@Test
	public void testTrain_helloWorld() {
		SymbolTable table = FsstTrain.train(List.of("hello hello hello"));

		ByteSlice encoded = table.encodeAll("Hello World");

		Assertions.assertThat(encoded.length).isEqualTo(15);

		ByteSlice decoded = table.decodeAll(encoded);
		String decodedString = new String(decoded.array, decoded.offset, decoded.length, StandardCharsets.UTF_8);

		Assertions.assertThat(decodedString).isEqualTo("Hello World");
	}

	@Test
	public void testTrain_hello() {
		SymbolTable table = FsstTrain.train(List.of("hello hello hello"));

		ByteSlice encoded = table.encodeAll("Hello");

		Assertions.assertThat(encoded.length).isEqualTo(6);

		ByteSlice decoded = table.decodeAll(encoded);
		String decodedString = new String(decoded.array, decoded.offset, decoded.length, StandardCharsets.UTF_8);

		Assertions.assertThat(decodedString).isEqualTo("Hello");
	}

	@Test
	public void testTrain_2codes() {
		SymbolTable table = FsstTrain.train(List.of("az_azaz_azazaz_azazazaz"));

		ByteSlice encoded = table.encodeAll("azaz");

		Assertions.assertThat(encoded.length).isEqualTo(4);

		ByteSlice decoded = table.decodeAll(encoded);
		String decodedString = new String(decoded.array, decoded.offset, decoded.length, StandardCharsets.UTF_8);

		Assertions.assertThat(decodedString).isEqualTo("azaz");
	}

	/**
	 * Consider a training given all bytes once then a single byte many times.
	 */
	@Test
	public void testTrain_allBytes() {
		byte[] input = new byte[256 * 3];

		// Choose a byte which is negative, as this is less usual and may spot some bug
		byte recurrent = (byte) 234;

		// All bytes are encountered once
		for (int i = 0; i < 256; i++) {
			input[i] = (byte) i;
		}
		// Some given byte is very frequent and consecutive
		for (int i = 256; i < 512; i++) {
			input[i] = recurrent;
		}

		SymbolTable table = FsstTrain.train(new byte[][] { input });

		byte[] original = new byte[] { recurrent, recurrent, recurrent, recurrent };
		ByteSlice encoded = table.encodeAll(original);

		Assertions.assertThat(encoded.length).isEqualTo(2);

		ByteSlice decoded = table.decodeAll(encoded);

		Assertions.assertThat(decoded.length).isEqualTo(original.length);
		Assertions.assertThat(decoded.array).contains(original);
	}

	@Test
	public void testSerialization() throws IOException, ClassNotFoundException {
		SymbolTable tableOriginal = FsstTrain.train(List.of("hello hello hello"));

		byte[] tableAsBytes = PepperSerializationHelper.toBytes(SymbolTableExternalizable.wrap(tableOriginal));
		SymbolTable table = PepperSerializationHelper.<SymbolTableExternalizable>fromBytes(tableAsBytes).symbolTable;

		ByteSlice encoded = table.encodeAll("Hello");

		Assertions.assertThat(encoded.length).isEqualTo(6);

		ByteSlice decoded = table.decodeAll(encoded);
		String decodedString = new String(decoded.array, decoded.offset, decoded.length, StandardCharsets.UTF_8);

		Assertions.assertThat(decodedString).isEqualTo("Hello");
	}

	private void testDataset(String filename) {
		String input = PepperResourceHelper.loadAsString("fsst/paper/dbtext/%s".formatted(filename));

		List<String> inputs = List.of(input.split("[\r\n]+"));

		SymbolTable table = FsstTrain.train(inputs);

		long sizeEncoded = 0;

		for (int i = 0; i < inputs.size(); i++) {
			String entry = inputs.get(i);
			ByteSlice encoded = table.encodeAll(entry);

			sizeEncoded += encoded.length;

			ByteSlice decoded = table.decodeAll(encoded);
			String decodedString = new String(decoded.array, decoded.offset, decoded.length, StandardCharsets.UTF_8);

			Assertions.assertThat(decodedString).isEqualTo(entry);
		}

		long sizeTable;
		try {
			sizeTable = PepperSerializationHelper.toBytes(SymbolTableExternalizable.wrap(table)).length;
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}

		log.info("Compressed {} from bytes={} to bytes={} (table={} encoded={})",
				filename,
				PepperLogHelper.humanBytes(input.getBytes(StandardCharsets.UTF_8).length),
				PepperLogHelper.humanBytes(sizeTable + sizeEncoded),
				PepperLogHelper.humanBytes(sizeTable),
				PepperLogHelper.humanBytes(sizeEncoded));
	}

	@Test
	public void testDataset_chinese() throws IOException, ClassNotFoundException {
		String filename = "chinese.txt";
		testDataset(filename);
	}

	@Test
	public void testDataset_city() throws IOException, ClassNotFoundException {
		String filename = "city.txt";
		testDataset(filename);
	}

	@Test
	public void testDataset_credentials() throws IOException, ClassNotFoundException {
		String filename = "credentials.txt";
		testDataset(filename);
	}
}
