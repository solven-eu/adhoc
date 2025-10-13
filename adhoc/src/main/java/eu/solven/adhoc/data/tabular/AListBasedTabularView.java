package eu.solven.adhoc.data.tabular;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import lombok.experimental.SuperBuilder;

@SuperBuilder
public abstract class AListBasedTabularView extends ATabularView {

	public void checkIsDistinct() {
		Set<Map<String, ?>> slices = new HashSet<>();

		slices().forEach(slice -> {
			Map<String, ?> coordinate = slice.getCoordinates();
			if (!slices.add(coordinate)) {
				throw new IllegalStateException("Multiple slices with c=%s. It is illegal in %s".formatted(coordinate,
						this.getClass().getName()));
			}
		});
	}
}
