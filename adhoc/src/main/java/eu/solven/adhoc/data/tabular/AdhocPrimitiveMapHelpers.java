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
package eu.solven.adhoc.data.tabular;

import java.util.Collection;

import eu.solven.adhoc.util.AdhocUnsafe;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import lombok.experimental.UtilityClass;

/**
 * 
 * Helps building {@link Collection}, especially if it relies on a primitive type.
 * 
 * @author Benoit Lacelle
 * 
 */
@UtilityClass
public class AdhocPrimitiveMapHelpers {

	public static <T> Object2IntMap<T> newHashMapDefaultMinus1() {
		return newHashMapDefaultMinus1(AdhocUnsafe.getDefaultColumnCapacity());
	}

	@SuppressWarnings("PMD.LooseCoupling")
	public static <T> Object2IntMap<T> newHashMapDefaultMinus1(int size) {
		Object2IntOpenHashMap<T> map = new Object2IntOpenHashMap<>(size);

		// If we request an unknown slice, we must not map to an existing index
		map.defaultReturnValue(-1);

		return map;
	}

}
