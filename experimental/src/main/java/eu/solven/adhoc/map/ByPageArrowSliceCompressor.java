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
package eu.solven.adhoc.map;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.table.Table;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Enable compression by relying on Arrow, and more specifically on Arrow dictionarization.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
public class ByPageArrowSliceCompressor {
	static final int ALLOCATOR_LIMIT = 8 * 1024;

	@RequiredArgsConstructor
	protected class ValueVectorOrObject {
		final FieldVector valueVector;
		final Object[] objects;
	}

	// TODO How to keep the allocator for the query lifetime?
	@SuppressWarnings({ "PMD.CloseResource", "PMD.LooseCoupling" })
	protected void doCompress(List<ProxiedAdhocMap> toCompress) {
		// super.doCompress(toCompress);

		// https://arrow.apache.org/docs/java/memory.html#reference-counting
		BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT);

		List<ValueVectorOrObject> vectors = new ArrayList<>();

		toCompress.getFirst().keySet().forEach(column -> {
			Set<?> classes = toCompress.stream()
					.map(s -> s.get(column))
					.filter(v -> v != null)
					.map(Object::getClass)
					.collect(Collectors.toSet());

			if (classes.size() == 1 && classes.contains(String.class)) {
				vectors.add(new ValueVectorOrObject(new VarCharVector(column, allocator), null));
			} else if (classes.size() == 1 && classes.contains(Boolean.class)) {
				vectors.add(new ValueVectorOrObject(new BitVector(column, allocator), null));
			} else {
				// fallback to a plain Object[] column
				vectors.add(new ValueVectorOrObject(null, toCompress.stream().map(s -> s.get(column)).toArray()));
			}

			ValueVector valueVector = vectors.getLast().valueVector;
			if (valueVector != null) {
				valueVector.allocateNew();

				AtomicInteger index = new AtomicInteger();
				toCompress.stream().map(s -> s.get(column)).forEach(o -> {
					if (valueVector instanceof VarCharVector varcharVector) {
						varcharVector.setSafe(index.getAndIncrement(), o.toString().getBytes(StandardCharsets.UTF_8));
					} else if (valueVector instanceof BitVector bitVector) {
						if (Boolean.TRUE.equals(o)) {
							bitVector.setSafe(index.getAndIncrement(), 1);
						} else {
							bitVector.setSafe(index.getAndIncrement(), 0);
						}
					} else {
						throw new IllegalStateException("valueVector=" + valueVector);
					}
				});

				valueVector.setValueCount(index.get());
			}
		});

		// OpaqueVector ?
		List<FieldVector> vectors2 = vectors.stream().map(v -> v.valueVector).filter(v -> v != null).toList();
		Table table = new Table(vectors2);
		log.info("table={}", table);
	}
}
