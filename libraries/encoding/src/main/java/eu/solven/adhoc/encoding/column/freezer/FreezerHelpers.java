package eu.solven.adhoc.encoding.column.freezer;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.experimental.UtilityClass;

/**
 * Utilities for {@link IFreezingWithContext}.
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
public class FreezerHelpers {

	@SuppressWarnings("checkstyle:AvoidInlineConditionals")
	public static Set<?> classesWithContext(Map<String, Object> freezingContext, List<?> array) {
		return (Set<?>) freezingContext.computeIfAbsent("classes",
				k -> array.stream().map(o -> o == null ? null : o.getClass()).collect(Collectors.toSet()));
	}
}
