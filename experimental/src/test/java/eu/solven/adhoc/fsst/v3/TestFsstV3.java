package eu.solven.adhoc.fsst.v3;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.fsst.v3.SymbolTable2.ByteSlice;
import eu.solven.adhoc.fsst.v3.SymbolUtil.Symbol;

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
		SymbolTable2 table = FsstTrain.train(List.of("hello hello hello"));

		ByteSlice encoded = table.encodeAll("Hello World");

		Assertions.assertThat(encoded.length).isEqualTo(15);

		ByteSlice decoded = table.decodeAll(encoded);
		String decodedString = new String(decoded.array, decoded.offset, decoded.length, StandardCharsets.UTF_8);

		Assertions.assertThat(decodedString).isEqualTo("Hello World");
	}

	@Test
	public void testTrain_hello() {
		SymbolTable2 table = FsstTrain.train(List.of("hello hello hello"));

		ByteSlice encoded = table.encodeAll("Hello");

		Assertions.assertThat(encoded.length).isEqualTo(6);

		ByteSlice decoded = table.decodeAll(encoded);
		String decodedString = new String(decoded.array, decoded.offset, decoded.length, StandardCharsets.UTF_8);

		Assertions.assertThat(decodedString).isEqualTo("Hello");
	}

	@Test
	public void testTrain_2codes() {
		SymbolTable2 table = FsstTrain.train(List.of("az_azaz_azazaz_azazazaz"));

		ByteSlice encoded = table.encodeAll("azaz");

		Assertions.assertThat(encoded.length).isEqualTo(4);

		ByteSlice decoded = table.decodeAll(encoded);
		String decodedString = new String(decoded.array, decoded.offset, decoded.length, StandardCharsets.UTF_8);

		Assertions.assertThat(decodedString).isEqualTo("azaz");
	}
}
