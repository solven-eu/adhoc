/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.measure.sum;

import java.util.Map;
import java.util.Objects;

import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.decomposition.IDecomposition;
import eu.solven.pepper.core.PepperLogHelper;
import eu.solven.pepper.mappath.MapPathGet;
import lombok.extern.slf4j.Slf4j;

/**
 * Typically used for advanced {@link IDecomposition} where we know there will be no actual aggregation.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
public class CoalesceAggregation implements IAggregation {

	public static final String KEY = "COALESCE";

	/**
	 * If true, equality is based on the reference, not on `.equals`. Default is false.
	 */
	boolean matchReferences;
	/**
	 * If true, `.aggregate` would fail if receiving different objects.
	 */
	boolean failIfDifferent;

	public CoalesceAggregation(Map<String, ?> options) {
		matchReferences = MapPathGet.<Boolean>getOptionalAs(options, "matchReferences").orElse(false);
		failIfDifferent = MapPathGet.<Boolean>getOptionalAs(options, "failIfDifferent").orElse(false);
	}

	public CoalesceAggregation() {
		this(Map.of());
	}

	@Override
	public Object aggregate(Object l, Object r) {
		if (l == null) {
			return r;
		} else if (r == null) {
			return l;
		} else if (areSimilar(l, r)) {
			return l;
		} else if (failIfDifferent) {
			if (Objects.equals(l, r) && matchReferences) {
				// Show identityHashCode if equals but rejected for different ref
				throw new IllegalArgumentException(
						"%s != %s (%s)".formatted(Objects.toIdentityString(l), Objects.toIdentityString(r), l));
			} else {
				throw new IllegalArgumentException("%s != %s".formatted(PepperLogHelper.getObjectAndClass(l),
						PepperLogHelper.getObjectAndClass(r)));
			}
		} else {
			// Give priority to left/first object
			return l;
		}
	}

	@SuppressWarnings("PMD.CompareObjectsWithEquals")
	protected boolean areSimilar(Object l, Object r) {
		if (matchReferences) {
			return l == r;
		} else {
			return Objects.equals(l, r);
		}
	}
}
