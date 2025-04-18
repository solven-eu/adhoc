package eu.solven.adhoc.pivotable.app.example;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import eu.solven.adhoc.dag.step.ISliceWithStep;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.pepper.mappath.MapPathGet;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link ICombination} returning the reference currency parameter. it can be used as underlying of other
 * {@link IMeasure}, or just used for the User to check this value.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
public class ReferenceCcyCombination implements ICombination {
	public static final String CCY_DEFAULT = "EUR";

	@Override
	public Object combine(ISliceWithStep slice, List<?> underlyingValues) {
		Object customMarker = slice.getQueryStep().getCustomMarker();

		if (customMarker == null) {
			return CCY_DEFAULT;
		} else if (customMarker instanceof Map<?, ?> asMap) {
			Optional<String> optReferenceCcy = MapPathGet.getOptionalString(asMap, "ccy");
			return optReferenceCcy.orElse(CCY_DEFAULT);
		} else {
			log.warn("Not-managed customMarker: {}", customMarker);
			return CCY_DEFAULT;
		}
	}
}
