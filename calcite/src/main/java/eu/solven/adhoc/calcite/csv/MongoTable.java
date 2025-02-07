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

import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeName;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.solven.adhoc.cube.IAdhocCubeWrapper;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.query.cube.IAdhocQuery;
import eu.solven.adhoc.storage.ITabularView;

/**
 * Table based on a MongoDB collection.
 */
public class MongoTable extends AbstractQueryableTable implements TranslatableTable {
	// private final String collectionName;
	final IAdhocCubeWrapper aqw;

	/** Creates a MongoTable. */
	MongoTable(IAdhocCubeWrapper aqw) {
		super(Object[].class);
		// this.collectionName = collectionName;
		this.aqw = aqw;
	}

	@Override
	public String toString() {
		return "AdhocTable {" + aqw + "}";
	}

	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory) {
		final RelDataType mapType = typeFactory.createMapType(typeFactory.createSqlType(SqlTypeName.VARCHAR),
				typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.ANY), true));
		return typeFactory.builder().add("_MAP", mapType).build();
	}

	@Override
	public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
		return new MongoQueryable<>(queryProvider, schema, this, tableName);
	}

	@Override
	public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
		final RelOptCluster cluster = context.getCluster();
		return new MongoTableScan(cluster, cluster.traitSetOf(MongoRel.CONVENTION), relOptTable, this, null);
	}

	/**
	 * Executes a "find" operation on the underlying collection.
	 *
	 * <p>
	 * For example, <code>zipsTable.find("{state: 'OR'}", "{city: 1, zipcode: 1}")</code>
	 *
	 * @param mongoDb
	 *            MongoDB connection
	 * @param filterJson
	 *            Filter JSON string, or null
	 * @param projectJson
	 *            Project JSON string, or null
	 * @param fields
	 *            List of fields to project; or null to return map
	 * @return Enumerator of results
	 */
	// private Enumerable<Object> find(MongoDatabase mongoDb,
	// String filterJson,
	// String projectJson,
	// List<Map.Entry<String, Class>> fields) {
	// final MongoCollection collection = mongoDb.getCollection(collectionName);
	// final Bson filter = filterJson == null ? null :
	// BsonDocument.parse(filterJson);
	// final Bson project = projectJson == null ? null :
	// BsonDocument.parse(projectJson);
	// final Function1<Document, Object> getter = MongoEnumerator.getter(fields);
	// return new AbstractEnumerable<Object>() {
	// @Override
	// public Enumerator<Object> enumerator() {
	// @SuppressWarnings("unchecked")
	// final FindIterable<Document> cursor =
	// collection.find(filter).projection(project);
	// return new MongoEnumerator(cursor.iterator(), getter);
	// }
	// };
	// }

	/**
	 * Executes an "aggregate" operation on the underlying collection.
	 *
	 * <p>
	 * For example: <code>zipsTable.aggregate(
	 * "{$filter: {state: 'OR'}",
	 * "{$group: {_id: '$city', c: {$sum: 1}, p: {$sum: '$pop'}}}")
	 * </code>
	 *
	 * @param mongoDb
	 *            MongoDB connection
	 * @param fields
	 *            List of fields to project; or null to return map
	 * @param adhocQuery
	 *            One or more JSON strings
	 * @return Enumerator of results
	 */
	private Enumerable<Object> aggregate(
			// final MongoDatabase mongoDb,
			final List<Map.Entry<String, Class>> fields,
			final IAdhocQuery adhocQuery) {
		final List<Object> list = new ArrayList<>();
		// for (String operation : operations) {
		// list.add(BsonDocument.parse(operation));
		// }
		final Function1<Map<String, ?>, Object> getter = MongoEnumerator.getter(fields);
		return new AbstractEnumerable<Object>() {
			@Override
			public Enumerator<Object> enumerator() {
				final Iterator<? extends Map<String, ?>> resultIterator;
				try {
					ITabularView result = aqw.execute(adhocQuery);

					resultIterator = result.slices().map(slice -> {
						return slice.getCoordinates();
					}).iterator();

					// mongoDb.getCollection(collectionName).aggregate(list).iterator();
				} catch (Exception e) {
					throw new RuntimeException("While running MongoDB query " + adhocQuery, e);
				}
				return new MongoEnumerator(resultIterator, getter);
			}
		};
	}

	/**
	 * Implementation of {@link org.apache.calcite.linq4j.Queryable} based on a
	 * {@link org.apache.calcite.adapter.mongodb.MongoTable}.
	 *
	 * @param <T>
	 *            element type
	 */
	public static class MongoQueryable<T> extends AbstractTableQueryable<T> {
		MongoQueryable(QueryProvider queryProvider, SchemaPlus schema, MongoTable table, String tableName) {
			super(queryProvider, schema, table, tableName);
		}

		@Override
		public Enumerator<T> enumerator() {
			// noinspection unchecked
			// final Enumerable<T> enumerable = (Enumerable<T>)
			// getTable().find(getMongoDb(), null, null, null);
			// return enumerable.enumerator();
			return null;
		}

		// private MongoDatabase getMongoDb() {
		// return schema.unwrap(MongoSchema.class).mongoDb;
		// }

		// private AdhocQueryEngine getTable() {
		// return (AdhocQueryEngine) table;
		// }
		private MongoTable getTable() {
			return (MongoTable) table;
		}

		/**
		 * Called via code-generation. See MongoMethod
		 *
		 * @see org.apache.calcite.adapter.mongodb.MongoMethod#MONGO_QUERYABLE_AGGREGATE
		 */
		@SuppressWarnings("UnusedDeclaration")
		public Enumerable<Object> aggregate(List<Map.Entry<String, Class>> fields, List adhocQuery) {
			Object rawQuery = adhocQuery.get(0);

			IAdhocQuery q;
			try {
				q = new ObjectMapper().readValue(rawQuery.toString(), AdhocQuery.class);
			} catch (JsonProcessingException e) {
				throw new UncheckedIOException(e);
			}
			return getTable().aggregate(
					// getMongoDb(),
					fields,
					q);
			// return null;
		}

		/**
		 * Called via code-generation.
		 *
		 * @param filterJson
		 *            Filter document
		 * @param projectJson
		 *            Projection document
		 * @param fields
		 *            List of expected fields (and their types)
		 * @return result of mongo query
		 *
		 * @see org.apache.calcite.adapter.mongodb.MongoMethod#MONGO_QUERYABLE_FIND
		 */
		@SuppressWarnings("UnusedDeclaration")
		public Enumerable<Object> find(String filterJson, String projectJson, List<Map.Entry<String, Class>> fields) {
			// return getTable().find(getMongoDb(), filterJson, projectJson, fields);
			return null;
		}
	}
}