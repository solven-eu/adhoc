package eu.solven.adhoc.beta.schema;

import java.util.Map;

import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.StringMatcher;
import eu.solven.adhoc.table.transcoder.value.ICustomTypeManagerSimple;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link ICustomTypeManagerSimple} typically used to transcode from raw String (e.g. from a JSON) to a proper
 * {@link IAdhocFilter}.
 * 
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Slf4j
public class CubeWrapperTypeTranscoder implements ICustomTypeManagerSimple {
	private final Map<String, Class<?>> cubeColumns;

	@Override
	public IValueMatcher toTable(String column, IValueMatcher valueMatcher) {
		return valueMatcher;
	}

	@Override
	public Object toTable(String column, Object coordinate) {
		if (coordinate == null) {
			// TODO Some cube would probably like transcoding null to `NULL`
			// To be clarified as `toTable` is called mainly on `EqualsMatcher` which does not accept `null`.
			return null;
		} else {
			Class<?> cubeColumnClass = cubeColumns.get(column);

			if (coordinate instanceof String coordinateAsString) {
				if (cubeColumnClass.equals(String.class)) {
					// column holds `String` and we filter a `String`
					return coordinate;
				} else {
					// One very generic but probably slow way to convert from String is to search matching
					// coordinates
					IValueMatcher stringMatcher = StringMatcher.hasToString(coordinateAsString);
					// CoordinatesSample sample =
					// cubeWrapper.getCoordinates(column, stringMatcher, Integer.MAX_VALUE);
					//
					// if (sample.getCoordinates().size() >= 2) {
					// // Is it legal to have multiple coordinates with same .toString?
					// // Should the filter be turned into an In?
					// log.warn("Multiple matches in cube={} column={} for `{}`: {}",
					// cubeWrapper.getName(),
					// column,
					// stringMatcher,
					// sample.getCoordinates());
					// }
					//
					// return sample.getCoordinates().iterator().next();

					// Transcode the type into an IValueMatcher: this may be handled efficiently by the table
					// e.g. PGSQL would turn `StringMatcher` into a `CAST column = 'searchedValue'`
					return stringMatcher;
				}
			} else {
				// column holds `!String` and we filter a `!String`: let's just check the type seems compatible

				if (!cubeColumnClass.isAssignableFrom(coordinate.getClass())) {
					log.warn("Filtering  v=`{}` on column={} which has type={}",
							PepperLogHelper.getObjectAndClass(coordinate),
							column,
							cubeColumnClass);
				}

				return coordinate;
			}
		}

	}

	@Override
	public boolean mayTranscode(String column) {
		return cubeColumns.containsKey(column);
	}
}