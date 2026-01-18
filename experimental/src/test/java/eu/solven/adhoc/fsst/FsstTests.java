package eu.solven.adhoc.fsst;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FsstTests {
	@Test
	void roundTrip() {
		byte[] data = "hello hello hello".getBytes(StandardCharsets.UTF_8);
		Assertions.assertThat(data).hasSize(17);

		SymbolTable table = FsstTrainer.train(data);

		FsstEncoder enc = new FsstEncoder(table);
		FsstDecoder dec = new FsstDecoder(table);

		byte[] compressed = enc.encode(data);

		Assertions.assertThat(compressed).hasSize(3);
		Assertions.assertThat(table.lookup).hasSize(42);
		Assertions.assertThat(table.byCode).hasSize(42);

		// Calculate table size (bytes)
		int tableSizeBytes = 0;
		for (Symbol s : table.byCode) {
			tableSizeBytes += s.getKey().length() + 1; // substring bytes + 1 for code
		}

		log.info("Symbol table size: {} bytes", tableSizeBytes);
		Assertions.assertThat(tableSizeBytes).isEqualTo(252);

		byte[] restored = dec.decode(compressed);
		Assertions.assertThat(restored).containsExactly(data);
	}

	@Test
	void roundTrip_withHigherBitSet() {
		byte[] data = "hello hello hello".getBytes(StandardCharsets.UTF_8);
		Assertions.assertThat(data).hasSize(17);

		SymbolTable table = FsstTrainer.train(data);

		FsstEncoder enc = new FsstEncoder(table);
		FsstDecoder dec = new FsstDecoder(table);

		byte[] dataWithAdditionalByte = Arrays.copyOf(data, data.length + 1);
		dataWithAdditionalByte[data.length] = -128;
		byte[] compressed = enc.encode(dataWithAdditionalByte);

		Assertions.assertThat(compressed).hasSize(5);
		// escape character
		Assertions.assertThat(compressed[compressed.length - 2]).isEqualTo((byte) 0xFF);
		// escaped character
		Assertions.assertThat(compressed[compressed.length - 1]).isEqualTo((byte) -128);
		Assertions.assertThat(table.lookup).hasSize(42);
		Assertions.assertThat(table.byCode).hasSize(42);

		// Calculate table size (bytes)
		int tableSizeBytes = 0;
		for (Symbol s : table.byCode) {
			tableSizeBytes += s.getKey().length() + 1; // substring bytes + 1 for code
		}

		log.info("Symbol table size: {} bytes", tableSizeBytes);
		Assertions.assertThat(tableSizeBytes).isEqualTo(252);

		byte[] restored = dec.decode(compressed);
		Assertions.assertThat(restored).containsExactly(dataWithAdditionalByte);
	}
}
