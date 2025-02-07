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
package eu.solven.adhoc.measure.decomposition.many2many;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;

import eu.solven.adhoc.query.filter.value.IValueMatcher;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ManyToManyNDInMemoryDefinition implements IManyToManyNDDefinition {
	final SetMultimap<Object, Map<String, IValueMatcher>> groupToElements =
			MultimapBuilder.SetMultimapBuilder.hashKeys().hashSetValues().build();

	// final SetMultimap<Map<String, ?>, Object> elementToGroups =
	// MultimapBuilder.SetMultimapBuilder.hashKeys().hashSetValues().build();

	@Override
	public Set<Object> getGroups(IValueMatcher groupMatcher, Map<String, ?> columnToElement) {
		return groupToElements.asMap().entrySet().stream().filter(e -> groupMatcher.match(e.getKey())).filter(e -> {
			Collection<Map<String, IValueMatcher>> matchers = e.getValue();

			return matchers.stream().anyMatch(matcher -> doElementMatch(matcher, columnToElement));
		}).map(e -> e.getKey()).collect(Collectors.toSet());
	}

	protected boolean doElementMatch(Map<String, IValueMatcher> matcher, Map<String, ?> columnToElement) {
		return matcher.entrySet().stream().allMatch(e -> {
			String column = e.getKey();
			Object value = columnToElement.get(column);
			return e.getValue().match(value);
		});
	}

	protected Stream<Object> streamMatchingGroups(IValueMatcher groupMatcher) {
		return groupToElements.keySet().stream().filter(groupMatcher::match);
	}

	@Override
	public Set<Map<String, IValueMatcher>> getElementsMatchingGroups(IValueMatcher groupMatcher) {
		Set<Map<String, IValueMatcher>> elementsMatchingGroups = new LinkedHashSet<>();

		streamMatchingGroups(groupMatcher).forEach(group -> elementsMatchingGroups.addAll(groupToElements.get(group)));

		log.debug("Mapped groupMatcher={} to #elements={} (elements={})",
				groupMatcher,
				elementsMatchingGroups.size(),
				elementsMatchingGroups);

		return elementsMatchingGroups;
	}

	@Override
	public Set<?> getMatchingGroups(IValueMatcher groupMatcher) {
		return streamMatchingGroups(groupMatcher).collect(Collectors.toSet());
	}

	public void putElementToGroup(Map<String, IValueMatcher> element, Object rawGroup) {
		if (rawGroup instanceof Collection<?> groups) {
			groups.forEach(group -> putElementToGroup(element, group));
		} else {
			// elementToGroups.put(element, rawGroup);
			groupToElements.put(rawGroup, element);
		}
	}

}
