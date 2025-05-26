package eu.solven.adhoc.measure.decomposition;

import java.util.Map;

import eu.solven.adhoc.data.cell.IValueProvider;

public interface IDecompositionEntry {
	Map<String, ?> getSlice();

	IValueProvider getValue();

	static IDecompositionEntry of(Map<String, ?> slice, IValueProvider value) {
		return DecompositionEntry.builder().slice(slice).value(value).build();
	}

	static IDecompositionEntry of(Map<String, ?> slice, Object value) {
		if (value instanceof IValueProvider valueProvider) {
			return of(slice, valueProvider);
		} else {
			return of(slice, IValueProvider.setValue(value));
		}
	}
}
