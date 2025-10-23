package eu.solven.adhoc.engine.context;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.core.env.Environment;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import eu.solven.adhoc.query.IQueryOption;
import eu.solven.adhoc.query.InternalQueryOptions;
import eu.solven.adhoc.query.cube.ICubeQuery;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Activates some options based on a Spring {@link Environment}.
 * 
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Slf4j
public class SpringImplicitOptions implements IImplicitOptions {
	final Map<String, List<? extends IQueryOption>> nameToPriority =
			ImmutableMap.<String, List<? extends IQueryOption>>builder()
					.put("adhoc.query.table.",
							// TODO Add sanity checks to detect conflicts (in environment)
							ImmutableList.<IQueryOption>builder()
									.add(InternalQueryOptions.ONE_TABLE_QUERY_PER_INDUCER)
									.add(InternalQueryOptions.ONE_TABLE_QUERY_PER_AGGREGATOR)
									.add(InternalQueryOptions.ONE_TABLE_QUERY_PER_ROOT_INDUCER)
									.build())
					.build();

	final Environment env;

	@Override
	public Set<IQueryOption> getOptions(ICubeQuery query) {
		Set<IQueryOption> options = new LinkedHashSet<>();

		nameToPriority.forEach((prefix, orderedOptions) -> {
			if (!Sets.intersection(query.getOptions(), orderedOptions.stream().collect(Collectors.toSet())).isEmpty()) {
				log.debug("Query has option {}, overriding any related option from the environment");
				return;
			}

			// Search for the first active option from environment
			List<? extends IQueryOption> activeOptions =
					orderedOptions.stream().filter(o -> isActive(prefix, o)).toList();

			// add it as option
			if (!activeOptions.isEmpty()) {
				if (activeOptions.size() >= 2) {
					throw new IllegalStateException("Conflicting options: " + activeOptions);
				} else {
					options.add(activeOptions.getFirst());
				}
			}
		});

		return options;
	}

	protected Boolean isActive(String prefix, IQueryOption o) {
		return env.getProperty(prefix + o.toString().toLowerCase(Locale.US), Boolean.class, false);
	}
}
