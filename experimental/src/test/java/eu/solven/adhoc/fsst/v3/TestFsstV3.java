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

	// If optimal, we should have a single symbol
	@Test
	public void testTrain_8chars() {
		SymbolTable table = FsstTrain.train(List.of("01234567"));

		ByteSlice encoded = table.encodeAll("01234567");

		Assertions.assertThat(encoded.length).isEqualTo(16);

		ByteSlice decoded = table.decodeAll(encoded);
		String decodedString = new String(decoded.array, decoded.offset, decoded.length, StandardCharsets.UTF_8);

		Assertions.assertThat(decodedString).isEqualTo("01234567");
	}

	@Test
	public void testTrain_helloWorld() {
		SymbolTable table = FsstTrain.train(List.of("hello hello hello"));

		ByteSlice encoded = table.encodeAll("Hello World");

		Assertions.assertThat(encoded.length).isEqualTo(22);

		ByteSlice decoded = table.decodeAll(encoded);
		String decodedString = new String(decoded.array, decoded.offset, decoded.length, StandardCharsets.UTF_8);

		Assertions.assertThat(decodedString).isEqualTo("Hello World");
	}

	// `hello` is a typically learnt symbol but not its substrings. Hence, `Hello` is not well encoded.
	// TODO Could we improve this? Should be feasible given the low number of different symbols.
	@Test
	public void testTrain_hello() {
		SymbolTable table = FsstTrain.train(List.of("hello hello hello"));

		ByteSlice encoded = table.encodeAll("Hello");

		Assertions.assertThat(encoded.length).isEqualTo(10);

		ByteSlice decoded = table.decodeAll(encoded);
		String decodedString = new String(decoded.array, decoded.offset, decoded.length, StandardCharsets.UTF_8);

		Assertions.assertThat(decodedString).isEqualTo("Hello");
	}

	@Test
	public void testTrain_2codes() {
		// 1x 2x 3x 4x
		SymbolTable table = FsstTrain.train(List.of("az_azaz_azazaz_azazazaz"));

		// 2x
		ByteSlice encoded = table.encodeAll("azaz");

		Assertions.assertThat(encoded.length).isEqualTo(2);

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

		Assertions.assertThat(encoded.length).isEqualTo(4);

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

		ByteSlice decoded = table.decodeAll(encoded);
		String decodedString = new String(decoded.array, decoded.offset, decoded.length, StandardCharsets.UTF_8);

		Assertions.assertThat(decodedString).isEqualTo("Hello");
	}

	// Once can increase this value to generate easily profilable benchmarks
	int nbRetryForBenchmark = 0;

	// This ensures nbRetryForBenchmark is not commited >0
	@Test
	public void testUnitTestsDoesNotRetryForBenchmarks() {
		Assertions.assertThat(nbRetryForBenchmark).isEqualTo(0);
	}

	private void testDataset(String filename) {
		String input = PepperResourceHelper.loadAsString("fsst/paper/dbtext/%s".formatted(filename));

		// Compress input as a single entry
		{
			SymbolTable table = FsstTrain.train(input);
			ByteSlice encoded = table.encodeAll(input);

			ByteSlice decoded = table.decodeAll(encoded);
			String decodedString = new String(decoded.array, decoded.offset, decoded.length, StandardCharsets.UTF_8);

			Assertions.assertThat(decodedString).isEqualTo(input);

			long sizeTable = byteLength(table);
			long sizeEncoded = encoded.length;
			log.info("row-basis Compressed {} from bytes={} to bytes={} (table={} encoded={})",
					filename,
					PepperLogHelper.humanBytes(input.getBytes(StandardCharsets.UTF_8).length),
					PepperLogHelper.humanBytes(sizeTable + sizeEncoded),
					PepperLogHelper.humanBytes(sizeTable),
					PepperLogHelper.humanBytes(sizeEncoded));
		}

		// Compress on a row basis, enabling random access
		{
			List<String> inputs = List.of(input.split("[\r\n]+"));

			for (int iRetry = 0; iRetry < nbRetryForBenchmark; iRetry++) {
				SymbolTable table = FsstTrain.train(inputs);

				long sizeEncoded = 0;

				for (int i = 0; i < inputs.size(); i++) {
					String entry = inputs.get(i);
					ByteSlice encoded = table.encodeAll(entry);

					sizeEncoded += encoded.length;

					ByteSlice decoded = table.decodeAll(encoded);
					String decodedString =
							new String(decoded.array, decoded.offset, decoded.length, StandardCharsets.UTF_8);

					Assertions.assertThat(decodedString).isEqualTo(entry);
				}

				if (iRetry == 0) {
					long sizeTable = byteLength(table);

					log.info("row-basis Compressed {} from bytes={} to bytes={} (table={} encoded={})",
							filename,
							PepperLogHelper.humanBytes(input.getBytes(StandardCharsets.UTF_8).length),
							PepperLogHelper.humanBytes(sizeTable + sizeEncoded),
							PepperLogHelper.humanBytes(sizeTable),
							PepperLogHelper.humanBytes(sizeEncoded));
				}
			}
		}
	}

	private long byteLength(SymbolTable table) {
		long sizeTable;
		try {
			sizeTable = PepperSerializationHelper.toBytes(SymbolTableExternalizable.wrap(table)).length;
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		return sizeTable;
	}

	@Test
	public void testDataset_chinese() {
		String filename = "chinese.txt";
		testDataset(filename);
	}

	@Test
	public void testDataset_city() {
		String filename = "city.txt";
		testDataset(filename);
	}

	@Test
	public void testDataset_credentials() {
		String filename = "credentials.txt";
		testDataset(filename);
	}
}
