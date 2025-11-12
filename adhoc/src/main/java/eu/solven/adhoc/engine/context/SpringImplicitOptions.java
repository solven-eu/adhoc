/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package eu.solven.adhoc.engine.context;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.core.env.Environment;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import eu.solven.adhoc.query.IQueryOption;
import eu.solven.adhoc.query.InternalQueryOptions;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.cube.ICubeQuery;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;

/**
 * Activates some options based on a Spring {@link Environment}.
 * 
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Slf4j
@Builder
public class SpringImplicitOptions implements IImplicitOptions {
	static final Map<String, Set<? extends IQueryOption>> NAME_TO_PRIORITY =
			ImmutableMap.<String, Set<? extends IQueryOption>>builder()
					.put("adhoc.query.table.",
							// TODO Add sanity checks to detect conflicts (in environment)
							ImmutableSet.<IQueryOption>builder()
									.add(InternalQueryOptions.ONE_TABLE_QUERY_PER_INDUCER)
									.add(InternalQueryOptions.ONE_TABLE_QUERY_PER_AGGREGATOR)
									.add(InternalQueryOptions.ONE_TABLE_QUERY_PER_ROOT_INDUCER)
									.build())
					.put("adhoc.query.",
							ImmutableSet.<IQueryOption>builder()
									.add(StandardQueryOptions.CONCURRENT,
											StandardQueryOptions.DEBUG,
											StandardQueryOptions.EXPLAIN,
											StandardQueryOptions.NO_CACHE,
											StandardQueryOptions.UNKNOWN_MEASURES_ARE_EMPTY)
									.build())
					.build();

	final Environment env;

	@Singular
	final ImmutableSet<IQueryOption> defaultOptions;

	public Set<IQueryOption> logDefaultOptions() {
		Set<IQueryOption> defaultOptions = getOptions(CubeQuery.builder().build());

		log.info("Queries has #{} default options: {}", defaultOptions.size(), defaultOptions);

		return defaultOptions;
	}

	@Override
	public Set<IQueryOption> getOptions(ICubeQuery query) {
		Set<IQueryOption> options = new LinkedHashSet<>();

		NAME_TO_PRIORITY.forEach((prefix, orderedOptions) -> {
			Set<IQueryOption> intersection =
					Sets.intersection(query.getOptions(), orderedOptions.stream().collect(Collectors.toSet()));
			if (!intersection.isEmpty()) {
				log.debug("Query has option {}, overriding any related option from the environment", intersection);
				return;
			}

			// Search for the first active option from environment
			List<? extends IQueryOption> activeOptions =
					orderedOptions.stream().filter(o -> isActiveInEnv(prefix, o)).toList();

			// No option from env: search in defaultOptions
			if (activeOptions.isEmpty()) {
				activeOptions = orderedOptions.stream().filter(defaultOptions::contains).toList();
			}

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

	protected Boolean isActiveInEnv(String prefix, IQueryOption o) {
		// By default, we read properties as lowerCase
		boolean isActiveLowerCase = env.getProperty(prefix + o.toString().toLowerCase(Locale.US), Boolean.class, false);

		// Fallback on upperCase, as some user may copyPast the enum value
		return isActiveLowerCase | env.getProperty(prefix + o.toString().toUpperCase(Locale.US), Boolean.class, false);
	}

	public String optionKey(IQueryOption option) {
		return NAME_TO_PRIORITY.entrySet()
				.stream()
				.filter(e -> e.getValue().contains(option))
				.map(e -> e.getKey() + option.toString().toLowerCase(Locale.US))
				.findFirst()
				.orElseThrow(() -> new IllegalArgumentException("Invalid option: " + option));
	}
}
