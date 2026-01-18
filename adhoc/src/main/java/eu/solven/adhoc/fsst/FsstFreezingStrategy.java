package eu.solven.adhoc.fsst;

import java.nio.charset.StandardCharsets;
import java.util.List;

import eu.solven.adhoc.compression.column.IAppendableColumn;
import eu.solven.adhoc.compression.column.ObjectArrayColumn;
import eu.solven.adhoc.compression.column.StandardFreezingStrategy;
import eu.solven.adhoc.compression.page.IReadableColumn;

/**
 * Extends {@link StandardFreezingStrategy} by enabling FSST over columns of {@link String}.
 * 
 * @author Benoit Lacelle
 * 
 */
public class FsstFreezingStrategy extends StandardFreezingStrategy {

	@Override
	public IReadableColumn freeze(IAppendableColumn column) {
		if (column instanceof ObjectArrayColumn arrayColumn) {
			List<?> array = arrayColumn.getAsArray();

			if (array.stream().allMatch(s -> s == null || CharSequence.class.isInstance(s))) {
				List<byte[]> primitiveArray = array.stream()
						.map(s -> s == null ? null : s.toString().getBytes(StandardCharsets.UTF_8))
						.toList();
				return FsstReadableColumn(primitiveArray);
			}
		}

		return super.freeze(column);
	}

	protected IReadableColumn FsstReadableColumn(List<byte[]> primitiveArray) {
		SymbolTable table = FsstTrainer.train(primitiveArray.iterator());

		FsstEncoder enc = new FsstEncoder(table);

		List<byte[]> encoded = primitiveArray.stream().map(bytes -> bytes == null ? null : enc.encode(bytes)).toList();

		FsstDecoder decoder = new FsstDecoder(table);
		return FsstReadableColumn.builder().decoder(decoder).encoded(encoded).build();
	}

}
