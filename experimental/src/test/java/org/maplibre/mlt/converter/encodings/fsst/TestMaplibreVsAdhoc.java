package org.maplibre.mlt.converter.encodings.fsst;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.fsst.v3.FsstAdhoc;
import eu.solven.pepper.spring.PepperResourceHelper;

public class TestMaplibreVsAdhoc {

	private static final byte[] SMALL = Base64.getDecoder().decode("SGV1YmFjaExpcHBlS2V0dGJhY2hTdGV2ZXI=");

	Fsst adhoc = new FsstAdhoc();
	Fsst java = new FsstJava();

	@Test
	public void testCompareCompression_SMALL() {
		SymbolTable encodedAdhoc = adhoc.encode(SMALL);
		SymbolTable encodedMaplibre = java.encode(SMALL);

		Assertions.assertThat(SMALL.length).isEqualTo(26L);
		Assertions.assertThat(encodedAdhoc.weight()).isEqualTo(42L);
		Assertions.assertThat(encodedMaplibre.weight()).isEqualTo(48L);

		Assertions.assertThat(adhoc.decode(encodedAdhoc)).contains(SMALL);
		Assertions.assertThat(java.decode(encodedMaplibre)).contains(SMALL);
	}

	@Test
	public void testCompareCompression_file() {
		String filename = "credentials.txt";
		String input = PepperResourceHelper.loadAsString("fsst/paper/dbtext/%s".formatted(filename));

		byte[] inputBytes = input.getBytes(StandardCharsets.UTF_8);
		SymbolTable encodedAdhoc = adhoc.encode(inputBytes);
		SymbolTable encodedMaplibre = java.encode(inputBytes);

		Assertions.assertThat(inputBytes.length).isEqualTo(154265L);
		Assertions.assertThat(encodedAdhoc.weight()).isEqualTo(98231L);
		Assertions.assertThat(encodedMaplibre.weight()).isEqualTo(73083L);

		Assertions.assertThat(adhoc.decode(encodedAdhoc)).contains(inputBytes);
		Assertions.assertThat(java.decode(encodedMaplibre)).contains(inputBytes);
	}
}
