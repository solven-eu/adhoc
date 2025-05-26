package eu.solven.adhoc.measure.decomposition;

import java.util.Map;

import eu.solven.adhoc.data.cell.IValueProvider;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class DecompositionEntry implements IDecompositionEntry {
	Map<String, ?> slice;

	IValueProvider value;
}
