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
package eu.solven.adhoc.beta.schema;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Set;

import org.jooq.impl.SQLDataType;

import com.google.common.collect.Sets;

import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;

@Value
@Builder
@Jacksonized
@Slf4j
public class ColumnarMetadata {
	public static final String VARCHAR = SQLDataType.VARCHAR.getCastTypeName();
	public static final String INTEGER = SQLDataType.INTEGER.getCastTypeName();
	public static final String LONG = SQLDataType.BIGINT.getCastTypeName();
	public static final String FLOAT = SQLDataType.FLOAT.getCastTypeName();
	public static final String DOUBLE = SQLDataType.DOUBLE.getCastTypeName();
	public static final String BOOLEAN = SQLDataType.BOOLEAN.getCastTypeName();

	public static final String OFFSETDATETIME = SQLDataType.OFFSETDATETIME.getCastTypeName();
	public static final String LOCALDATE = SQLDataType.LOCALDATE.getCastTypeName();
	public static final String DATE = SQLDataType.DATE.getCastTypeName();

	public static final String BLOB = SQLDataType.BLOB.getCastTypeName();

	static final Set<Map.Entry<String, Class<?>>> UNCLEAR_TYPE_WARNED = Sets.newConcurrentHashSet();

	public static void clearWarns() {
		UNCLEAR_TYPE_WARNED.clear();
	}

	@Singular
	Map<String, String> columnToTypes;

	public static ColumnarMetadata from(Map<String, Class<?>> columns) {
		ColumnarMetadataBuilder builder = ColumnarMetadata.builder();

		columns.forEach((name, clazz) -> {
			if (CharSequence.class.isAssignableFrom(clazz)) {
				builder.columnToType(name, VARCHAR);
			} else if (LocalDate.class.isAssignableFrom(clazz)) {
				builder.columnToType(name, LOCALDATE);
			} else if (Integer.class.isAssignableFrom(clazz)) {
				builder.columnToType(name, INTEGER);
			} else if (Long.class.isAssignableFrom(clazz)) {
				builder.columnToType(name, LONG);
			} else if (Double.class.isAssignableFrom(clazz)) {
				builder.columnToType(name, DOUBLE);
			} else if (Float.class.isAssignableFrom(clazz)) {
				builder.columnToType(name, FLOAT);
			} else if (BigDecimal.class.isAssignableFrom(clazz)) {
				builder.columnToType(name, DOUBLE);
			} else if (java.util.Date.class.isAssignableFrom(clazz)) {
				builder.columnToType(name, DATE);
			} else if (OffsetDateTime.class.isAssignableFrom(clazz)) {
				builder.columnToType(name, OFFSETDATETIME);
			} else if (Boolean.class.isAssignableFrom(clazz)) {
				builder.columnToType(name, BOOLEAN);
			} else {
				if (UNCLEAR_TYPE_WARNED.add(Map.entry(name, clazz))) {
					log.warn("Unclear type for name={} clazz={}", name, clazz);
				}
				builder.columnToType(name, BLOB);
			}
		});

		return builder.build();
	}

}
