/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.map.perfect_hashing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import com.google.errorprone.annotations.ThreadSafe;

import eu.solven.adhoc.encoding.fsst.IFsstConstants;
import eu.solven.adhoc.util.immutable.IImmutable;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

/**
 * A simple perfect-hashing strategy.
 * 
 * Instead of relying on modulo arithmetic, we will consider only powerOfTwo mask, for faster `hash->index`, as masking
 * is faster than a modulo operation. This is a requirement to remain same order of performance than {@link HashMap}.
 * 
 * @author Benoit Lacelle
 */
@Builder
@Slf4j
@ThreadSafe
public class SimplePerfectHash<T> implements IHasIndexOf<T>, IImmutable {
	private static final int HASH_PRIME = (int) IFsstConstants.fsstHashPrime;
	// Most JVMs has constrain over the largest array, even if a lot of RAM/Heap is available
	private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 128;
	private static final int BITS_PER_INT = 32;

	final List<T> table;
	final int mask;
	final int[] remapped;
	final int shift;

	// TODO JMH to compare with HashMap
	// In Adhoc, it is a small deal as these are created in very small number
	public static <T> IHasIndexOf<T> make(Collection<T> keys) {
		int n = keys.size();
		if (keys.stream().distinct().count() < n) {
			// Invalid input, else we would always encounter a collision, preventing a perfect hash
			throw new IllegalArgumentException("Inputs must be distinct");
		} else if (keys.stream().mapToInt(Object::hashCode).distinct().count() < n) {
			return HashMapIndexOf.make(keys);
		}

		if (n == 0) {
			return SimplePerfectHash.<T>builder().table(List.of()).mask(0).remapped(new int[] { 0 }).build();
		}

		List<T> keyList = new ArrayList<>(keys);

		// TODO ensure this return the same value is already a power of two, else the following powerOfTwo
		int nbSlots = higherOrEqualsPowerOfTwo(n);

		while (true) {
			int mask = nbSlots - 1;

			if (mask < 0) {
				throw new IllegalArgumentException("Can not find hashing parameters for keys=" + keys);
			} else if (nbSlots > MAX_ARRAY_SIZE) {
				throw new IllegalArgumentException(
						"Can not find hashing parameters (nbSlots=%s) for keys=%s".formatted(nbSlots, keys));
			}

			T[] rawCandidates;
			int[] remapped;
			try {
				rawCandidates = (T[]) new Object[nbSlots];
				remapped = new int[nbSlots];
			} catch (OutOfMemoryError e) {
				// TODO have a policy to prevent anyway this structure to grow too large
				log.error("nbSlots={} led to {} for keys={}", nbSlots, e.getClass().getName(), keys, e);
				throw e;
			}
			List<T> candidate = Arrays.<T>asList(rawCandidates);
			Arrays.fill(remapped, -1);

			int maxShift = Integer.numberOfLeadingZeros(mask);

			// We try shifting the mask, as it may lead to a valid perfect hash without having to grow the table
			for (int shift = 0; shift <= maxShift; shift++) {
				boolean collision = false;

				int originalIndex = 0;
				for (T key : keyList) {
					// Evaluate the index given the hash and current parameters
					int indexOf = ((key.hashCode() * HASH_PRIME) & (mask << shift)) >>> shift;

					// Register the candidate to detect collisions
					T already = candidate.set(indexOf, key);
					if (already != null) {
						log.debug(
								"Collision at index={} mask={} shift={} between {} (hashCode={}) and {} (hashCode={})",
								originalIndex,
								mask,
								shift,
								already,
								already.hashCode(),
								key,
								key.hashCode());
						collision = true;
						break;
					}

					remapped[indexOf] = originalIndex;
					originalIndex++;
				}

				if (!collision) {
					return SimplePerfectHash.<T>builder()
							.table(candidate)
							.mask(mask)
							.shift(shift)
							.remapped(remapped)
							.build();
				} else {
					Arrays.fill(rawCandidates, null);
					Arrays.fill(remapped, -1);
				}
			}

			// TODO We should try with various shift as it may have the same cost to shift the mask, and would help to
			// keep a smaller table
			nbSlots <<= 1; // try next size
		}
	}

	static int higherOrEqualsPowerOfTwo(int n) {
		return 1 << (BITS_PER_INT - Integer.numberOfLeadingZeros(n - 1));
	}

	/**
	 * @param key
	 * @return the index in the original {@link Set}, or -1.
	 */
	@Override
	public int indexOf(T key) {
		int index = ((key.hashCode() * HASH_PRIME) & (mask << shift)) >>> shift;

		// Even if the index looks valid, we need to check the input is not a wrong entry with a conflicting hashcode
		if (index >= table.size() || !key.equals(table.get(index))) {
			// not found
			return -1;
		}
		return remapped[index];
	}

	/**
	 * May be slightly faster than {@link #indexOf(String)}, but you need to ensure you're calling a valid key.
	 * 
	 * @param key
	 * @return the index in the original {@link Set}
	 */
	@Override
	public int unsafeIndexOf(T key) {
		int index = ((key.hashCode() * HASH_PRIME) & (mask << shift)) >>> shift;
		return remapped[index];
	}
}