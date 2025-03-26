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

import java.lang.reflect.Method;
import java.util.List;

import org.apache.calcite.linq4j.tree.Types;

import com.google.common.collect.ImmutableMap;

/**
 * Builtin methods in the MongoDB adapter.
 */
public enum MongoMethod {
	// MONGO_QUERYABLE_FIND(MongoTable.MongoQueryable.class, "find", String.class,
	// String.class, List.class),
	MONGO_QUERYABLE_AGGREGATE(AdhocCalciteTable.MongoQueryable.class, "aggregate", List.class, List.class);

	@SuppressWarnings("ImmutableEnumChecker")
	public final Method method;

	public static final ImmutableMap<Method, MongoMethod> MAP;

	static {
		final ImmutableMap.Builder<Method, MongoMethod> builder = ImmutableMap.builder();
		for (MongoMethod value : MongoMethod.values()) {
			builder.put(value.method, value);
		}
		MAP = builder.build();
	}

	MongoMethod(Class clazz, String methodName, Class... argumentTypes) {
		this.method = Types.lookupMethod(clazz, methodName, argumentTypes);
	}
}