package eu.solven.adhoc.fsst.v3;

import java.nio.charset.StandardCharsets;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.fsst.v3.SymbolUtil.Symbol;

public class TestSymbol {

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
	public void testToString() {
		Symbol symbol = Symbol.newSymbolFromBytes("abc".getBytes(StandardCharsets.UTF_8));

		Assertions.assertThat(symbol).hasToString("length=3 code=0 value=616263");
	}

	@Test
	public void testToString_0ispadded() {
		Symbol symbol = Symbol.newSymbolFromBytes(new byte[] { 0 });

		Assertions.assertThat(symbol).hasToString("length=1 code=0 value=00");
	}

	@Test
	public void testToString_01ispadded() {
		Symbol symbol = Symbol.newSymbolFromBytes(new byte[] { 0, 1 });

		Assertions.assertThat(symbol).hasToString("length=2 code=0 value=1000");
	}

	// TODO Something is wrong: we should not have same string as `01` case
	@Test
	public void testToString_10ispadded() {
		Symbol symbol = Symbol.newSymbolFromBytes(new byte[] { 1, 0 });

		Assertions.assertThat(symbol).hasToString("length=2 code=0 value=1000");
	}
}
