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
package eu.solven.adhoc.calcite.csv;

import static java.lang.String.format;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;

import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.Primitive;
import org.checkerframework.checker.nullness.qual.Nullable;

import eu.solven.adhoc.data.row.ITabularRecord;

/** Enumerator that reads from a MongoDB collection. */
class MongoEnumerator implements Enumerator<Object> {
	private final List<Entry<String, Class<?>>> fields;
	private final Iterator<? extends ITabularRecord> cursor;
	private @Nullable Object current;

	/**
	 * Creates a MongoEnumerator.
	 * 
	 * @param fields
	 *
	 * @param cursor
	 *            Mongo iterator (usually a {@link com.mongodb.DBCursor})
	 * @param getter
	 *            Converts an object into a list of fields
	 */
	MongoEnumerator(List<Entry<String, Class<?>>> fields, Iterator<? extends ITabularRecord> cursor) {
		this.fields = fields;
		this.cursor = cursor;
	}

	@Override
	public Object current() {
		if (current == null) {
			throw new IllegalStateException();
		}
		return current;
	}

	@Override
	public boolean moveNext() {
		try {
			if (cursor.hasNext()) {
				ITabularRecord map = cursor.next();

				if (fields.size() == 1) {
					current = map.getAggregate(fields.getFirst().getKey());
					// TODO Cast to proper type given `.getValue`
				} else {
					current = fields.stream().map(e -> map.getAggregate(e.getKey())).toArray();
				}

				return true;
			} else {
				current = null;
				return false;
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void reset() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() {
		if (cursor instanceof Closeable closeable) {
			try {
				closeable.close();
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		} else if (cursor instanceof AutoCloseable autoCloseable) {
			try {
				autoCloseable.close();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		// AggregationOutput implements Iterator but not DBCursor. There is no
		// available close() method -- apparently there is no open resource.
	}

	/**
	 * Converts the given object to a specific runtime type based on the provided class.
	 *
	 * @param fieldName
	 *            The name of the field being processed, used for error reporting if conversion fails.
	 * @param o
	 *            The object to be converted. If `null`, the method returns `null` immediately.
	 * @param clazz
	 *            The target class to which the object `o` should be converted.
	 * @return The converted object as an instance of the specified `clazz`, or `null` if `o` is `null`.
	 *
	 * @throws IllegalArgumentException
	 *             if the object `o` cannot be converted to the desired `clazz` type, including a message indicating the
	 *             field name, expected data type, and the invalid value.
	 *
	 *             <h3>Conversion Details:</h3>
	 *
	 *             <p>
	 *             If the target type is one of the following, the method performs specific conversions:
	 *             <ul>
	 *             <li>`Long`: Converts a `Date` or `BsonTimestamp` object into the respective epoch time
	 *             (milliseconds).
	 *             <li>`BigDecimal`: Converts a `Decimal128` object into a `BigDecimal` instance.
	 *             <li>`String`: Converts arrays to string and uses `String.valueOf(o)` for other objects.
	 *             <li>`ByteString`: Converts a `Binary` object into a `ByteString` instance.
	 *             <li>`Primitive or Boxed Primitive`:
	 *             <ul>
	 *             <li>If the object is a `String`, it will be converted to the corresponding boxed primitive using
	 *             {@link Primitive#parse}.</li>
	 *             <li>If the object is numeric, it will be converted to the boxed primitive type using
	 *             {@link Primitive#number}.</li>
	 *             </ul>
	 *             </li>
	 *             </ul>
	 */
	@SuppressWarnings("JavaUtilDate")
	private static Object convert(String fieldName, Object o, Class clazz) {
		if (o == null) {
			return null;
		}
		Primitive primitive = Primitive.of(clazz);
		if (primitive != null) {
			clazz = primitive.boxClass;
		} else {
			primitive = Primitive.ofBox(clazz);
		}
		if (clazz.isInstance(o)) {
			return o;
		}

		if (clazz == Long.class) {
			if (o instanceof Date) {
				return ((Date) o).getTime();
			}
			// else if (o instanceof BsonTimestamp) {
			// return ((BsonTimestamp) o).getTime() * DateTimeUtils.MILLIS_PER_SECOND;
			// }
		} else if (clazz == BigDecimal.class) {
			// if (o instanceof Decimal128) {
			// return new BigDecimal(((Decimal128) o).toString());
			// }
		} else if (clazz == String.class) {
			if (o.getClass().isArray()) {
				return Primitive.OTHER.arrayToString(o);
			} else {
				return String.valueOf(o);
			}
		} else if (clazz == ByteString.class) {
			// if (o instanceof Binary) {
			// return new ByteString(((Binary) o).getData());
			// }
		}

		if (primitive != null) {
			if (o instanceof String) {
				return primitive.parse((String) o);
			} else if (o instanceof Number) {
				return primitive.number((Number) o);
			} else if (o instanceof Date) {
				return primitive.number(((Date) o).getTime() / DateTimeUtils.MILLIS_PER_DAY);
			}
		}

		throw new IllegalArgumentException(format(Locale.ROOT,
				"Invalid field: '%s'. The dataType '%s' is invalid for '%s'.",
				fieldName,
				clazz.getSimpleName(),
				o));
	}
}