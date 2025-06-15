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
package eu.solven.adhoc.table.duckdb;

import java.sql.SQLException;
import java.util.Map;
import java.util.regex.Pattern;

import org.assertj.core.api.Assertions;
import org.jooq.AggregateFunction;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Name;
import org.jooq.TableLike;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.util.concurrent.AtomicLongMap;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.column.ColumnsManager;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.data.cell.IValueReceiver;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.AdhocTestHelper;
import eu.solven.adhoc.engine.CubeQueryEngine;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.measure.aggregation.carrier.IAggregationCarrier;
import eu.solven.adhoc.measure.aggregation.collection.AtomicLongMapAggregation;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.table.sql.IJooqTableQueryFactory;
import eu.solven.adhoc.table.sql.JooqTableQueryFactory;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapperParameters;
import eu.solven.adhoc.table.sql.duckdb.DuckDbHelper;
import eu.solven.adhoc.util.IAdhocEventBus;
import lombok.Builder;
import lombok.Value;

/**
 * Check an advanced scenario where we receive String values, and we want to aggregate them as
 * {@link com.google.common.util.concurrent.AtomicLongMap}.
 */
public class TestTableQuery_DuckDb_customAggregation extends ADuckDbJooqTest implements IAdhocTestConstants {

	String tableName = "someTableName";

	JooqTableWrapper table;

	@Value
	@Builder
	public static class AtomicLongMapCarrier implements IAggregationCarrier {

		AtomicLongMap<Object> map;

		@Override
		public void acceptValueReceiver(IValueReceiver valueReceiver) {
			valueReceiver.onObject(map);
		}
	}

	/**
	 * Some custom aggregation which handles raw String from the table, and converts them to aggregable AtomicLongMap.
	 */
	public static class CustomMapAggregation extends AtomicLongMapAggregation
			implements IAggregationCarrier.IHasCarriers {

		@Override
		protected AtomicLongMap<?> asMap(Object o) {
			if (o instanceof String s) {
				AtomicLongMap<Object> map = AtomicLongMap.create();

				Pattern.compile(";").matcher(s).results().forEach(mr -> {
					String group = mr.group();

					int indexOfEquals = group.indexOf('=');

					String key = group.substring(0, indexOfEquals);
					long count = Long.parseLong(group.substring(indexOfEquals + 1));
					map.addAndGet(key, count);
				});

				return map;
			}
			return (AtomicLongMap<?>) o;
		}

		@Override
		public IAggregationCarrier wrap(Object v) {
			if (v instanceof String s) {
				AtomicLongMap<Object> map = AtomicLongMap.create();

				splitToMap(s, map);

				return AtomicLongMapCarrier.builder().map(map).build();
			} else if (v instanceof java.sql.Array array) {
				AtomicLongMap<Object> map = AtomicLongMap.create();

				try {
					for (Object o : ((Object[]) array.getArray())) {
						String s = o.toString();

						splitToMap(s, map);
					}
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}

				return AtomicLongMapCarrier.builder().map(map).build();
			} else {
				throw new UnsupportedOperationException("o=%s".formatted(v));
			}
		}

		/**
		 *
		 * @param s
		 *            a raw value like `a=123;b=234`
		 * @param map
		 *            the target map to fill
		 */
		private static void splitToMap(String s, AtomicLongMap<Object> map) {
			Pattern.compile(";").splitAsStream(s).forEach(group -> {
				int indexOfEquals = group.indexOf('=');

				String key = group.substring(0, indexOfEquals);
				long count = Long.parseLong(group.substring(indexOfEquals + 1));
				map.addAndGet(key, count);
			});
		}
	}

	public static class CustomAggregationJooqTableQueryFactory extends JooqTableQueryFactory {

		public CustomAggregationJooqTableQueryFactory(IOperatorFactory operatorFactory,
				TableLike<?> table,
				DSLContext dslContext,
				boolean canGroupByAll) {
			super(operatorFactory, table, dslContext, canGroupByAll);
		}

		@Override
		protected AggregateFunction<?> onCustomAggregation(Aggregator aggregator, Name namedColumn) {
			if (aggregator.getAggregationKey().equals(CustomMapAggregation.class.getName())) {
				Field<?> field = DSL.field(namedColumn);

				// https://duckdb.org/docs/stable/sql/functions/aggregates.html#array_aggarg
				return DSL.aggregate("array_agg", Object.class, field);
			}

			return super.onCustomAggregation(aggregator, namedColumn);
		}
	}

	{
		JooqTableWrapperParameters jooqTableWrapperParameters = JooqTableWrapperParameters.builder()
				.dslSupplier(DuckDbHelper.inMemoryDSLSupplier())
				.tableName(tableName)
				.build();
		table = new JooqTableWrapper(tableName, jooqTableWrapperParameters) {
			@Override
			protected IJooqTableQueryFactory makeQueryFactory(DSLContext dslContext) {
				return new CustomAggregationJooqTableQueryFactory(jooqTableWrapperParameters.getOperatorFactory(),
						jooqTableWrapperParameters.getTable(),
						dslContext,
						true);
			}
		};
	}

	DSLContext dsl = table.makeDsl();

	private CubeWrapper wrapInCube(IMeasureForest forest) {
		IAdhocEventBus adhocEventBus = AdhocTestHelper.eventBus()::post;
		CubeQueryEngine engine = CubeQueryEngine.builder().eventBus(adhocEventBus).build();

		ColumnsManager columnsManager = ColumnsManager.builder().eventBus(adhocEventBus).build();
		return CubeWrapper.builder()
				.engine(engine)
				.forest(forest)
				.table(table)
				.engine(engine)
				.columnsManager(columnsManager)
				.build();
	}

	String m = "key:count";

	@BeforeEach
	public void initAndInsert() {
		dsl.createTableIfNotExists(tableName)
				.column("color", SQLDataType.VARCHAR)
				.column("key:count", SQLDataType.VARCHAR)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field(DSL.quotedName("key:count")), DSL.field("color"))
				.values("k1=123", "blue")
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field(DSL.quotedName("key:count")), DSL.field("color"))
				.values("k2=234", "red")
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field(DSL.quotedName("key:count")), DSL.field("color"))
				.values("k1=345;k3=456", "blue")
				.execute();

		forest.addMeasure(Aggregator.countAsterisk())
				.addMeasure(Aggregator.builder().name(m).aggregationKey(CustomMapAggregation.class.getName()).build());
	}

	@Test
	public void testGetColumns() {
		Assertions.assertThat(wrapInCube(forest).getColumnTypes())
				.hasSize(2)
				.containsEntry("key:count", String.class)
				.containsEntry("color", String.class);
	}

	@Test
	public void testGrandTotal() {
		// groupBy `a` with no measure: this is a distinct query on given groupBy
		ITabularView result = wrapInCube(forest).execute(CubeQuery.builder().measure(m, "count(*)").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasEntrySatisfying(Map.of(), values -> {
			Assertions.assertThat((Map) values).containsEntry("count(*)", 0L + 3).hasEntrySatisfying(m, count -> {
				Assertions.assertThat(count).isInstanceOfSatisfying(AtomicLongMap.class, alm -> {
					Map<?, Long> atomicLongMap = alm.asMap();
					Assertions.assertThat((Map) atomicLongMap)
							.containsEntry("k1", 0L + 123 + 345)
							.containsEntry("k2", 0L + 234)
							.containsEntry("k3", 0L + 456)
							.hasSize(3);
				});
			});
		}).hasSize(1);
	}

	@Test
	public void testGroupBy() {
		// groupBy `a` with no measure: this is a distinct query on given groupBy
		ITabularView result =
				wrapInCube(forest).execute(CubeQuery.builder().groupByAlso("color").measure(m, "count(*)").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasEntrySatisfying(Map.of("color", "blue"), values -> {
			Assertions.assertThat((Map) values).containsEntry("count(*)", 0L + 2).hasEntrySatisfying(m, count -> {
				Assertions.assertThat(count).isInstanceOfSatisfying(AtomicLongMap.class, alm -> {
					Map<?, Long> atomicLongMap = alm.asMap();
					Assertions.assertThat((Map) atomicLongMap)
							.containsEntry("k1", 0L + 123 + 345)
							.containsEntry("k3", 0L + 456)
							.hasSize(2);
				});
			});
		}).hasEntrySatisfying(Map.of("color", "red"), values -> {
			Assertions.assertThat((Map) values).containsEntry("count(*)", 0L + 1).hasEntrySatisfying(m, count -> {
				Assertions.assertThat(count).isInstanceOfSatisfying(AtomicLongMap.class, alm -> {
					Map<?, Long> atomicLongMap = alm.asMap();
					Assertions.assertThat((Map) atomicLongMap).containsEntry("k2", 0L + 234).hasSize(1);
				});
			});
		}).hasSize(2);
	}

}
