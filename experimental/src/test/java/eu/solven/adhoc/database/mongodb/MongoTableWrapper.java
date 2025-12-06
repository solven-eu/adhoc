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
package eu.solven.adhoc.database.mongodb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

import eu.solven.adhoc.column.ColumnMetadata;
import eu.solven.adhoc.data.row.ITabularRecordStream;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.query.table.TableQueryV2;
import eu.solven.adhoc.table.ITableWrapper;
import lombok.Builder;
import lombok.Getter;

/**
 * A {@link ITableWrapper} over {@link MongoClient}.
 * 
 * @author Benoit Lacelle
 */
@Builder
public class MongoTableWrapper implements ITableWrapper {
	@Getter
	final String name;

	final MongoClient mongoClient = MongoClients.create("");
	final MongoDatabase database = mongoClient.getDatabase("dbName");

	@Override
	public Collection<ColumnMetadata> getColumns() {
		List<ColumnMetadata> columns = new ArrayList<>();

		SetMultimap<String, Class<?>> columnToTypes = MultimapBuilder.treeKeys().linkedHashSetValues().build();
		Set<String> isNullable = new TreeSet<>();

		database.getCollection("someCollection").find().limit(1).forEach(d -> {
			d.entrySet().forEach(entry -> {
				String key = entry.getKey();
				Object value = entry.getValue();
				if (value == null) {
					isNullable.add(key);
				} else {
					columnToTypes.put(key, value.getClass());
				}
			});
		});

		return columns;
	}

	@Override
	public ITabularRecordStream streamSlices(QueryPod queryPod, TableQueryV2 tableQuery) {
		// TODO Auto-generated method stub
		return null;
	}

}
