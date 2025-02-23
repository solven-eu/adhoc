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
package eu.solven.adhoc.column;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;

import eu.solven.adhoc.util.NotYetImplementedException;

// https://www.baeldung.com/jackson-custom-serialization
public class GroupByColumnsDeserializer extends StdDeserializer<NavigableMap<String, IAdhocColumn>> {
	private static final long serialVersionUID = 1L;

	public GroupByColumnsDeserializer() {
		this(null);
	}

	public GroupByColumnsDeserializer(Class<NavigableMap<String, IAdhocColumn>> t) {
		super(t);
	}

	@Override
	public Object deserializeWithType(JsonParser p, DeserializationContext ctxt, TypeDeserializer typeDeserializer)
			throws IOException {
		if (p.isExpectedStartArrayToken()) {
			return deserialize(p, ctxt);
		} else {
			// How to rely on the default deserialization with type?
			throw new NotYetImplementedException("Issue parsing a groupBy as object");
		}
	}

	@Override
	public NavigableMap<String, IAdhocColumn> deserialize(JsonParser p, DeserializationContext ctxt)
			throws IOException, JacksonException {
		if (p.isExpectedStartArrayToken()) {
			NavigableMap<String, IAdhocColumn> nameToColumn = new TreeMap<>();

			JsonNode array = ctxt.readTree(p);

			for (JsonNode node : array) {
				String str = node.textValue();

				nameToColumn.put(str, ReferencedColumn.ref(str));
			}

			return nameToColumn;
		} else {
			throw new NotYetImplementedException("Issue with not simple groupBy");
		}
	}
}