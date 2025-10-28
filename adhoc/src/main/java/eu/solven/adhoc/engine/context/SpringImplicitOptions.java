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
			Set<IQueryOption> intersection =
					Sets.intersection(query.getOptions(), orderedOptions.stream().collect(Collectors.toSet()));
			if (!intersection.isEmpty()) {
				log.debug("Query has option {}, overriding any related option from the environment", intersection);
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

	public String optionKey(InternalQueryOptions option) {
		return nameToPriority.entrySet()
				.stream()
				.filter(e -> e.getValue().contains(option))
				.map(e -> e.getKey() + option.name().toLowerCase(Locale.US))
				.findFirst()
				.orElseThrow(() -> new IllegalArgumentException("Invalid option: " + option));
	}
}
