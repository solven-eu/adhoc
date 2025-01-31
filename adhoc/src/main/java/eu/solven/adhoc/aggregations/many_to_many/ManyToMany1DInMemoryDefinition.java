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
package eu.solven.adhoc.aggregations.many_to_many;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;

import eu.solven.adhoc.query.filter.value.IValueMatcher;
import lombok.extern.slf4j.Slf4j;

/**
 * A simple {@link IManyToMany1DDefinition} which is based on {@link SetMultimap}. This is typically used by unittests,
 * or simple/small/config-based manytomany.
 * 
 * @author Benoit Lacelle
 *
 */
@Slf4j
public class ManyToMany1DInMemoryDefinition implements IManyToMany1DDefinition {
	final SetMultimap<Object, Object> groupToElements =
			MultimapBuilder.SetMultimapBuilder.hashKeys().hashSetValues().build();
	final SetMultimap<Object, Object> elementToGroups =
			MultimapBuilder.SetMultimapBuilder.hashKeys().hashSetValues().build();

	@Override
	public Set<Object> getGroups(Object element) {
		return elementToGroups.get(element);
	}

	protected Stream<Object> streamMatchingGroups(IValueMatcher groupMatcher) {
		return groupToElements.keySet().stream().filter(groupMatcher::match);
	}

	@Override
	public Set<?> getElementsMatchingGroups(IValueMatcher groupMatcher) {
		Set<Object> elementsMatchingGroups = new LinkedHashSet<>();

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

	// @Override
	// public Set<Object> getElements(Object group) {
	// return groupToElements.get(group);
	// }

	public void putElementToGroup(Object element, Object group) {
		elementToGroups.put(element, group);
		groupToElements.put(group, element);
	}
}
