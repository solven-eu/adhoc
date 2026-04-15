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
package eu.solven.adhoc.dataframe.column.hash;

import eu.solven.adhoc.primitive.AdhocPrimitiveHelpers;
import eu.solven.adhoc.primitive.IValueReceiver;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * An {@link IValueReceiver} which will clean input dirty values (e.g. turning a boxed {@link Integer} into a primitive
 * `int`).
 * 
 * @author Benoit Lacelle
 */
@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class CleaningValueReceiver implements IValueReceiver {
	public static final boolean DEFAULT = true;

	boolean cleanIfDirty;
	boolean cleanIfNull;

	final IValueReceiver decorated;

	public static IValueReceiver cleaning(boolean cleanIfDirty, boolean cleanIfNull, IValueReceiver decorated) {
		if (cleanIfDirty || cleanIfNull) {
			return new CleaningValueReceiver(cleanIfDirty, cleanIfNull, decorated);
		} else {
			return decorated;
		}
	}

	@Override
	public void onLong(long v) {
		decorated.onLong(v);
	}

	@Override
	public void onDouble(double v) {
		decorated.onDouble(v);
	}

	@Override
	public void onObject(Object v) {
		if (v == null) {
			if (cleanIfNull) {
				// BEWARE We may want to remove the key
				// BEWARE We may want to have an optimized storage for `null||long` or `null||double`
				log.trace("TODO Improve null management");
			} else {
				decorated.onObject(v);
			}
		} else if (AdhocPrimitiveHelpers.isLongLike(v)) {
			long vAsPrimitive = AdhocPrimitiveHelpers.asLong(v);
			decorated.onLong(vAsPrimitive);
		} else if (AdhocPrimitiveHelpers.isDoubleLike(v)) {
			double vAsPrimitive = AdhocPrimitiveHelpers.asDouble(v);
			decorated.onDouble(vAsPrimitive);
		} else {
			decorated.onObject(v);
		}
	}
}