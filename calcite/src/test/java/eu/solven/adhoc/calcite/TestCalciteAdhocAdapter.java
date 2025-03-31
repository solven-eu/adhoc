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
package eu.solven.adhoc.calcite;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.eventbus.EventBus;

import eu.solven.adhoc.calcite.csv.AdhocCalciteSchemaFactory;
import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.dag.ExecutingQueryContext;
import eu.solven.adhoc.eventbus.AdhocEventsFromGuavaEventBusToSfl4j;
import eu.solven.adhoc.measure.MeasureForest;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.table.InMemoryTable;
import eu.solven.pepper.spring.PepperResourceHelper;

/**
 * Testing mongo adapter functionality. By default, runs with Mongo Java Server unless {@code IT} maven profile is
 * enabled (via {@code $ mvn -Pit install}).
 *
 * @see MongoDatabasePolicy
 */
// TODO Rely on ADagTest
// @Disabled
public class TestCalciteAdhocAdapter {

	/** Connection factory based on the "mongo-zips" model. */
	protected static final Resource MODEL = new ClassPathResource("/calcite_model-adhoc.json");

	public final EventBus eventBus = new EventBus();
	public final AdhocEventsFromGuavaEventBusToSfl4j toSlf4j = new AdhocEventsFromGuavaEventBusToSfl4j();
	public final MeasureForest amb = MeasureForest.builder().name(this.getClass().getSimpleName()).build();
	public final AdhocQueryEngine aqe = AdhocQueryEngine.builder().eventBus(eventBus::post).build();

	public final InMemoryTable rows = InMemoryTable.builder().build();
	public final InMemoryTable zips = InMemoryTable.builder().build();

	@BeforeEach
	public void wireEvents() {
		eventBus.register(toSlf4j);

		rows.add(Map.of("k1", 123, "a", "a1"));
		AdhocCalciteSchemaFactory.nameToTable.put("adhoc_table", rows);

		String resourcePath = "zips-mini.json";
		String zipsString = PepperResourceHelper.loadAsString(resourcePath, StandardCharsets.UTF_8);

		ObjectMapper om = new ObjectMapper();
		Stream.of(zipsString.split("[\r\n]+")).filter(s -> !s.startsWith("//")).forEach(row -> {
			try {
				Map<String, ?> asMap = om.readValue(row, Map.class);
				zips.add(asMap);
			} catch (JsonProcessingException e) {
				throw new IllegalStateException("Issue processing %s from %s".formatted(row, "zips-mini.json"), e);
			}
		});
		AdhocCalciteSchemaFactory.nameToTable.put("zips", zips);
	}

	private CalciteAssert.AssertThat assertModel(Resource resource) {
		// ensure that Schema from this instance is being used
		// model = model.replace(MongoSchemaFactory.class.getName(), MongoAdapterTest.class.getName());

		try {
			return CalciteAssert.that()
					.withModel(resource.getURL())
					.with(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.name())
					.with(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.name());
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	@Test
	void testCountGroupByEmpty() {
		assertModel(MODEL).query("select count(*) from \"adhoc_schema\".\"adhoc_table\"")
				.returns(String.format(Locale.ROOT,
						"EXPR$0=%d\n",
						rows.streamSlices(ExecutingQueryContext.forTable(rows), TableQuery.builder().build())
								.asMap()
								.count()))
				.explainContains("""
						PLAN=MongoToEnumerableConverter
						  AdhocCalciteAggregate(group=[{}], EXPR$0=[COUNT()])
						    AdhocCalciteTableScan(table=[[adhoc_schema, adhoc_table]])
						""")
				.queryContains(mongoChecker("{$group: {_id: {}, 'EXPR$0': {$sum: 1}}}"));
	}

	@Test
	void testSumK1GroupBya() {
		assertModel(MODEL).query("select sum(k1) from \"adhoc_schema\".\"adhoc_table\" GROUP BY a")
				.returns(String.format(Locale.ROOT, "EXPR$0=%d\n", 123))
				.explainContains("""
						PLAN=EnumerableCalc(expr#0..1=[{inputs}], EXPR$0=[$t1])
						  MongoToEnumerableConverter
						    AdhocCalciteAggregate(group=[{0}], EXPR$0=[$SUM0($1)])
						      AdhocCalciteTableScan(table=[[adhoc_schema, adhoc_table]])
						""")
				.queryContains(mongoChecker("{$group: {_id: {}, 'EXPR$0': {$sum: 1}}}"));
	}

	@Disabled("Adhoc")
	@Test
	void testSort() {
		assertModel(MODEL).query("select * from \"adhoc_schema\".\"zips\" order by state")
				.returnsCount(0)
				.explainContains("PLAN=MongoToEnumerableConverter\n" + "  MongoSort(sort0=[$4], dir0=[ASC])\n"
						+ "    MongoProject(CITY=[CAST(ITEM($0, 'city')):VARCHAR(20)], LONGITUDE=[CAST(ITEM(ITEM($0, 'loc'), 0)):FLOAT], LATITUDE=[CAST(ITEM(ITEM($0, 'loc'), 1)):FLOAT], POP=[CAST(ITEM($0, 'pop')):INTEGER], state=[CAST(ITEM($0, 'state')):VARCHAR(2)], ID=[CAST(ITEM($0, '_id')):VARCHAR(5)])\n"
						+ "      MongoTableScan(table=[[mongo_raw, zips]])");
	}

	@Disabled("Adhoc")
	@Test
	void testSortLimit() {
		assertModel(MODEL)
				.query("select state, id from \"adhoc_schema\".\"zips\"\n"
						+ "order by state, id offset 2 rows fetch next 3 rows only")
				.returnsOrdered("state=AK; ID=99801", "state=AL; ID=35215", "state=AL; ID=35401")
				.queryContains(mongoChecker("{$project: {STATE: '$state', ID: '$_id'}}",
						"{$sort: {STATE: 1, ID: 1}}",
						"{$skip: 2}",
						"{$limit: 3}"));
	}

	@Disabled("Adhoc")
	@Test
	void testOffsetLimit() {
		assertModel(MODEL)
				.query("select state, id from \"adhoc_schema\".\"zips\"\n" + "offset 2 fetch next 3 rows only")
				.runs()
				.queryContains(mongoChecker("{$skip: 2}", "{$limit: 3}", "{$project: {STATE: '$state', ID: '$_id'}}"));
	}

	@Disabled("Adhoc")
	@Test
	void testLimit() {
		assertModel(MODEL).query("select state, id from \"adhoc_schema\".\"zips\"\n" + "fetch next 3 rows only")
				.runs()
				.queryContains(mongoChecker("{$limit: 3}", "{$project: {STATE: '$state', ID: '$_id'}}"));
	}

	@Disabled
	@Test
	void testFilterSort() {
		// LONGITUDE and LATITUDE are null because of CALCITE-194.
		Util.discard(Bug.CALCITE_194_FIXED);
		assertModel(MODEL)
				.query("select * from \"adhoc_schema\".\"zips\"\n" + "where city = 'SPRINGFIELD' and id >= '70000'\n"
						+ "order by state, id")
				.returns("" + "CITY=SPRINGFIELD; LONGITUDE=null; LATITUDE=null; POP=752; state=AR; ID=72157\n"
						+ "CITY=SPRINGFIELD; LONGITUDE=null; LATITUDE=null; POP=1992; state=CO; ID=81073\n"
						+ "CITY=SPRINGFIELD; LONGITUDE=null; LATITUDE=null; POP=5597; state=LA; ID=70462\n"
						+ "CITY=SPRINGFIELD; LONGITUDE=null; LATITUDE=null; POP=32384; state=OR; ID=97477\n"
						+ "CITY=SPRINGFIELD; LONGITUDE=null; LATITUDE=null; POP=27521; state=OR; ID=97478\n")
				.queryContains(mongoChecker(
						"{\n" + "  $match: {\n"
								+ "    city: \"SPRINGFIELD\",\n"
								+ "    _id: {\n"
								+ "      $gte: \"70000\"\n"
								+ "    }\n"
								+ "  }\n"
								+ "}",
						"{$project: {CITY: '$city', LONGITUDE: '$loc[0]', LATITUDE: '$loc[1]', POP: '$pop', STATE: '$state', ID: '$_id'}}",
						"{$sort: {STATE: 1, ID: 1}}"))
				.explainContains("PLAN=MongoToEnumerableConverter\n"
						+ "  MongoSort(sort0=[$4], sort1=[$5], dir0=[ASC], dir1=[ASC])\n"
						+ "    MongoProject(CITY=[CAST(ITEM($0, 'city')):VARCHAR(20)], LONGITUDE=[CAST(ITEM(ITEM($0, 'loc'), 0)):FLOAT], LATITUDE=[CAST(ITEM(ITEM($0, 'loc'), 1)):FLOAT], POP=[CAST(ITEM($0, 'pop')):INTEGER], state=[CAST(ITEM($0, 'state')):VARCHAR(2)], ID=[CAST(ITEM($0, '_id')):VARCHAR(5)])\n"
						+ "      MongoFilter(condition=[AND(=(CAST(ITEM($0, 'city')):VARCHAR(20), 'SPRINGFIELD'), >=(CAST(ITEM($0, '_id')):VARCHAR(5), '70000'))])\n"
						+ "        MongoTableScan(table=[[mongo_raw, zips]])");
	}

	@Disabled("Adhoc")
	@Test
	void testFilterSortDesc() {
		assertModel(MODEL)
				.query("select * from \"adhoc_schema\".\"zips\"\n" + "where pop BETWEEN 45000 AND 46000\n"
						+ "order by state desc, pop")
				.limit(4)
				.returnsOrdered("CITY=BECKLEY; LONGITUDE=null; LATITUDE=null; POP=45196; state=WV; ID=25801",
						"CITY=ROCKERVILLE; LONGITUDE=null; LATITUDE=null; POP=45328; state=SD; ID=57701",
						"CITY=PAWTUCKET; LONGITUDE=null; LATITUDE=null; POP=45442; state=RI; ID=02860",
						"CITY=LAWTON; LONGITUDE=null; LATITUDE=null; POP=45542; state=OK; ID=73505");
	}

	@Disabled("broken; [CALCITE-2115] is logged to fix it")
	@Test
	void testUnionPlan() {
		assertModel(MODEL)
				.query("select * from \"sales_fact_1997\"\n" + "union all\n" + "select * from \"sales_fact_1998\"")
				.explainContains("PLAN=EnumerableUnion(all=[true])\n" + "  MongoToEnumerableConverter\n"
						+ "    MongoProject(product_id=[CAST(ITEM($0, 'product_id')):DOUBLE])\n"
						+ "      MongoTableScan(table=[[_foodmart, sales_fact_1997]])\n"
						+ "  MongoToEnumerableConverter\n"
						+ "    MongoProject(product_id=[CAST(ITEM($0, 'product_id')):DOUBLE])\n"
						+ "      MongoTableScan(table=[[_foodmart, sales_fact_1998]])")
				.limit(2)
				.returns(CalciteAssert.checkResultContains("product_id=337", "product_id=1512"));
	}

	@Disabled("java.lang.ClassCastException: java.lang.Integer cannot be cast to java.lang.Double")
	@Test
	void testFilterUnionPlan() {
		assertModel(MODEL).query("select * from (\n" + "  select * from \"sales_fact_1997\"\n"
				+ "  union all\n"
				+ "  select * from \"sales_fact_1998\")\n"
				+ "where \"product_id\" = 1").runs();
	}

	/**
	 * Tests that mongo query is empty when filter simplified to false.
	 */
	@Test
	void testFilterRedundant() {
		assertModel(MODEL)
				.query("select * from \"adhoc_schema\".\"zips\" where state > 'CA' and state < 'AZ' and state = 'OK'")
				.runs()
				.queryContains(mongoChecker());
	}

	@Disabled("Adhoc")
	@Test
	void testSelectWhere() {
		assertModel(MODEL).query("select * from \"warehouse\" where \"warehouse_state_province\" = 'CA'")
				.explainContains("PLAN=MongoToEnumerableConverter\n"
						+ "  MongoProject(warehouse_id=[CAST(ITEM($0, 'warehouse_id')):DOUBLE], warehouse_state_province=[CAST(ITEM($0, 'warehouse_state_province')):VARCHAR(20)])\n"
						+ "    MongoFilter(condition=[=(CAST(ITEM($0, 'warehouse_state_province')):VARCHAR(20), 'CA')])\n"
						+ "      MongoTableScan(table=[[mongo_raw, warehouse]])")
				.returns(CalciteAssert.checkResultContains("warehouse_id=6; warehouse_state_province=CA",
						"warehouse_id=7; warehouse_state_province=CA",
						"warehouse_id=14; warehouse_state_province=CA",
						"warehouse_id=24; warehouse_state_province=CA"))
				.queryContains(
						// Per https://issues.apache.org/jira/browse/CALCITE-164,
						// $match must occur before $project for good performance.
						mongoChecker(
								"{\n" + "  \"$match\": {\n"
										+ "    \"warehouse_state_province\": \"CA\"\n"
										+ "  }\n"
										+ "}",
								"{$project: {warehouse_id: 1, warehouse_state_province: 1}}"));
	}

	@Disabled("Adhoc")
	@Test
	void testInPlan() {
		assertModel(MODEL).query("select \"store_id\", \"store_name\" from \"store\"\n"
				+ "where \"store_name\" in ('Store 1', 'Store 10', 'Store 11', 'Store 15', 'Store 16', 'Store 24', 'Store 3', 'Store 7')")
				.returns(CalciteAssert.checkResultContains("store_id=1; store_name=Store 1",
						"store_id=3; store_name=Store 3",
						"store_id=7; store_name=Store 7",
						"store_id=10; store_name=Store 10",
						"store_id=11; store_name=Store 11",
						"store_id=15; store_name=Store 15",
						"store_id=16; store_name=Store 16",
						"store_id=24; store_name=Store 24"))
				.queryContains(mongoChecker("{\n" + "  \"$match\": {\n"
						+ "    \"$or\": [\n"
						+ "      {\n"
						+ "        \"store_name\": \"Store 1\"\n"
						+ "      },\n"
						+ "      {\n"
						+ "        \"store_name\": \"Store 10\"\n"
						+ "      },\n"
						+ "      {\n"
						+ "        \"store_name\": \"Store 11\"\n"
						+ "      },\n"
						+ "      {\n"
						+ "        \"store_name\": \"Store 15\"\n"
						+ "      },\n"
						+ "      {\n"
						+ "        \"store_name\": \"Store 16\"\n"
						+ "      },\n"
						+ "      {\n"
						+ "        \"store_name\": \"Store 24\"\n"
						+ "      },\n"
						+ "      {\n"
						+ "        \"store_name\": \"Store 3\"\n"
						+ "      },\n"
						+ "      {\n"
						+ "        \"store_name\": \"Store 7\"\n"
						+ "      }\n"
						+ "    ]\n"
						+ "  }\n"
						+ "}", "{$project: {store_id: 1, store_name: 1}}"));
	}

	/** Simple query based on the "mongo-zips" model. */
	@Disabled("Adhoc")
	@Test
	void testZips() {
		assertModel(MODEL).query("select state, city from \"adhoc_schema\".\"zips\"").returnsCount(0);
	}

	@Disabled("Adhoc")
	@Test
	void testCountGroupByEmptyMultiplyBy2() {
		assertModel(MODEL).query("select count(*)*2 from \"adhoc_schema\".\"zips\"")
				.returns(String.format(Locale.ROOT, "EXPR$0=%d\n", 0 * 2))
				.queryContains(mongoChecker("{$group: {_id: {}, _0: {$sum: 1}}}",
						"{$project: {'EXPR$0': {$multiply: ['$_0', {$literal: 2}]}}}"));
	}

	@Test
	void testGroupByOneColumnNotProjected() {
		assertModel(MODEL).query("select count(*) from \"adhoc_schema\".\"zips\" group by state order by 1")
				.limit(2)
				.returnsUnordered("EXPR$0=2", "EXPR$0=2")
				.queryContains(mongoChecker("{$project: {STATE: '$state'}}",
						"{$group: {_id: '$STATE', 'EXPR$0': {$sum: 1}}}",
						"{$project: {STATE: '$_id', 'EXPR$0': '$EXPR$0'}}",
						"{$project: {'EXPR$0': 1}}",
						"{$sort: {EXPR$0: 1}}"));
	}

	// @Disabled("Adhoc")
	@Test
	void testGroupByOneColumn() {
		assertModel(MODEL)
				.query("select state, count(*) as C from \"adhoc_schema\".\"zips\" group by state order by state")
				.limit(3)
				.returns("state=AK; C=3\nstate=AL; C=3\nstate=AR; C=3\n")
				.queryContains(mongoChecker("{$project: {state: '$state'}}",
						"{$group: {_id: '$STATE', C: {$sum: 1}}}",
						"{$project: {STATE: '$_id', C: '$C'}}",
						"{$sort: {STATE: 1}}"));
	}

	// @Disabled("Adhoc")
	@Test
	void testGroupByOneColumnReversed() {
		// Note extra $project compared to testGroupByOneColumn.
		assertModel(MODEL)
				.query("select count(*) as C, state from \"adhoc_schema\".\"zips\" group by state order by state")
				.limit(2)
				.returns("C=3; state=AK\nC=3; state=AL\n")
				.queryContains(mongoChecker("{$project: {STATE: '$state'}}",
						"{$group: {_id: '$STATE', C: {$sum: 1}}}",
						"{$project: {STATE: '$_id', C: '$C'}}",
						"{$project: {C: 1, STATE: 1}}",
						"{$sort: {STATE: 1}}"));
	}

	// @Disabled("Adhoc")
	@Test
	void testGroupByAvg() {
		assertModel(MODEL)
				.query("select state, avg(pop) as A from \"adhoc_schema\".\"zips\" group by state order by state")
				.limit(2)
				.returns("state=AK; A=26856\nstate=AL; A=43383\n")
				.queryContains(mongoChecker("{$project: {STATE: '$state', POP: '$pop'}}",
						"{$group: {_id: '$STATE', A: {$avg: '$POP'}}}",
						"{$project: {STATE: '$_id', A: '$A'}}",
						"{$sort: {STATE: 1}}"));
	}

	@Disabled("Adhoc")
	@Test
	void testGroupByAvg_NY() {
		assertModel(MODEL)
				.query("select state, avg(pop) as A from \"adhoc_schema\".\"zips_NY\" group by state order by state")
				.limit(2)
				.returns("state=NY; A=12345")
				.queryContains(mongoChecker("{$project: {STATE: '$state', POP: '$pop'}}",
						"{$group: {_id: '$STATE', A: {$avg: '$POP'}}}",
						"{$project: {STATE: '$_id', A: '$A'}}",
						"{$sort: {STATE: 1}}"));
	}

	// The AVG may be computed by Calcite, instead of being requested directly to the table
	@Disabled("Adhoc")
	@Test
	void testGroupByAvgSumCount() {
		assertModel(MODEL).query(
				"select state, avg(pop) as A, sum(pop) as S, count(pop) as C from \"adhoc_schema\".\"zips\" group by state order by state")
				.limit(2)
				.returns("state=AK; A=26856; S=80568; C=3\n" + "state=AL; A=43383; S=130151; C=3\n")
				.queryContains(mongoChecker("{$project: {STATE: '$state', POP: '$pop'}}",
						"{$group: {_id: '$STATE', _1: {$sum: '$POP'}, _2: {$sum: {$cond: [ {$eq: ['POP', null]}, 0, 1]}}}}",
						"{$project: {STATE: '$_id', _1: '$_1', _2: '$_2'}}",
						"{$project: {STATE: 1, A: {$divide: [{$cond:[{$eq: ['$_2', {$literal: 0}]},null,'$_1']}, '$_2']}, S: {$cond:[{$eq: ['$_2', {$literal: 0}]},null,'$_1']}, C: '$_2'}}",
						"{$sort: {STATE: 1}}"));
	}

	// In this case, we need the filter to be handled by Calcite
	// Or improve Adhoc to rely on HAVING
	@Disabled("Adhoc")
	@Test
	void testGroupByHaving() {
		assertModel(MODEL)
				.query("select state, count(*) as c from \"adhoc_schema\".\"zips\"\n"
						+ "group by state having count(*) > 2 order by state")
				.returnsCount(47)
				.queryContains(mongoChecker("{$project: {STATE: '$state'}}",
						"{$group: {_id: '$STATE', C: {$sum: 1}}}",
						"{$project: {STATE: '$_id', C: '$C'}}",
						"{\n" + "  \"$match\": {\n"
								+ "    \"C\": {\n"
								+ "      \"$gt\": 2\n"
								+ "    }\n"
								+ "  }\n"
								+ "}",
						"{$sort: {STATE: 1}}"));
	}

	@Disabled("https://issues.apache.org/jira/browse/CALCITE-270")
	@Test
	void testGroupByHaving2() {
		assertModel(MODEL)
				.query("select state, count(*) as c from \"adhoc_schema\".\"zips\"\n"
						+ "group by state having sum(pop) > 12000000")
				.returns("state=NY; C=1596\n" + "state=TX; C=1676\n" + "state=FL; C=826\n" + "state=CA; C=1523\n")
				.queryContains(mongoChecker("{$project: {STATE: '$state', POP: '$pop'}}",
						"{$group: {_id: '$STATE', C: {$sum: 1}, _2: {$sum: '$POP'}}}",
						"{$project: {STATE: '$_id', C: '$C', _2: '$_2'}}",
						"{\n" + "  $match: {\n" + "    _2: {\n" + "      $gt: 12000000\n" + "    }\n" + "  }\n" + "}",
						"{$project: {STATE: 1, C: 1}}"));
	}

	@Test
	void testGroupByMinMaxSum() {
		assertModel(MODEL)
				.query("select count(*) as C, state,\n"
						+ " min(pop) as min_pop, max(pop) as max_pop, sum(pop) as sum_pop\n"
						+ "from \"adhoc_schema\".\"zips\" group by state order by state")
				.limit(2)
				.returns("C=3; state=AK; min_pop=23238; max_pop=32383; sum_pop=80568\n"
						+ "C=3; state=AL; min_pop=42124; max_pop=44165; sum_pop=130151\n")
				.queryContains(mongoChecker("{$project: {STATE: '$state', POP: '$pop'}}",
						"{$group: {_id: '$STATE', C: {$sum: 1}, MIN_POP: {$min: '$POP'}, MAX_POP: {$max: '$POP'}, SUM_POP: {$sum: '$POP'}}}",
						"{$project: {STATE: '$_id', C: '$C', MIN_POP: '$MIN_POP', MAX_POP: '$MAX_POP', SUM_POP: '$SUM_POP'}}",
						"{$project: {C: 1, STATE: 1, MIN_POP: 1, MAX_POP: 1, SUM_POP: 1}}",
						"{$sort: {STATE: 1}}"));
	}

	@Test
	void testGroupComposite() {
		assertModel(MODEL)
				.query("select count(*) as C, state, city from \"adhoc_schema\".\"zips\"\n" + "group by state, city\n"
						+ "order by C desc, city\n"
						+ "limit 2")
				.returns("""
						C=1; state=SD; city=ABERDEEN
						C=1; state=SC; city=AIKEN
						""")
				.queryContains(mongoChecker("{$project: {STATE: '$state', CITY: '$city'}}",
						"{$group: {_id: {STATE: '$STATE', CITY: '$CITY'}, C: {$sum: 1}}}",
						"{$project: {_id: 0, STATE: '$_id.STATE', CITY: '$_id.CITY', C: '$C'}}",
						"{$sort: {C: -1, CITY: 1}}",
						"{$limit: 2}",
						"{$project: {C: 1, STATE: 1, CITY: 1}}"));
	}

	@Disabled("broken; [CALCITE-2115] is logged to fix it")
	@Test
	void testDistinctCount() {
		assertModel(MODEL)
				.query("select state, count(distinct city) as cdc from \"adhoc_schema\".\"zips\"\n"
						+ "where state in ('CA', 'TX') group by state order by state")
				.returns("state=CA; CDC=1072\n" + "state=TX; CDC=1233\n")
				.queryContains(mongoChecker(
						"{\n" + "  \"$match\": {\n"
								+ "    \"$or\": [\n"
								+ "      {\n"
								+ "        \"state\": \"CA\"\n"
								+ "      },\n"
								+ "      {\n"
								+ "        \"state\": \"TX\"\n"
								+ "      }\n"
								+ "    ]\n"
								+ "  }\n"
								+ "}",
						"{$project: {CITY: '$city', STATE: '$state'}}",
						"{$group: {_id: {CITY: '$CITY', STATE: '$STATE'}}}",
						"{$project: {_id: 0, CITY: '$_id.CITY', STATE: '$_id.STATE'}}",
						"{$group: {_id: '$STATE', CDC: {$sum: {$cond: [ {$eq: ['CITY', null]}, 0, 1]}}}}",
						"{$project: {STATE: '$_id', CDC: '$CDC'}}",
						"{$sort: {STATE: 1}}"));
	}

	@Test
	void testDistinctCountOrderBy() {
		assertModel(MODEL)
				.query("select state, count(distinct city) as cdc\n" + "from \"adhoc_schema\".\"zips\"\n"
						+ "group by state\n"
						+ "order by cdc desc, state\n"
						+ "limit 5")
				.returns("""
						state=AK; cdc=3
						state=AL; cdc=3
						state=AR; cdc=3
						state=AZ; cdc=3
						state=CA; cdc=3
						""")
				.queryContains(mongoChecker("{$project: {CITY: '$city', STATE: '$state'}}",
						"{$group: {_id: {CITY: '$CITY', STATE: '$STATE'}}}",
						"{$project: {_id: 0, CITY: '$_id.CITY', STATE: '$_id.STATE'}}",
						"{$group: {_id: '$STATE', CDC: {$sum: {$cond: [ {$eq: ['CITY', null]}, 0, 1]}}}}",
						"{$project: {STATE: '$_id', CDC: '$CDC'}}",
						"{$sort: {CDC: -1, STATE: 1}}",
						"{$limit: 5}"));
	}

	@Disabled("broken; [CALCITE-2115] is logged to fix it")
	@Test
	void testProject() {
		assertModel(MODEL).query("select state, city, 0 as zero from \"adhoc_schema\".\"zips\" order by state, city")
				.limit(2)
				.returns("state=AK; CITY=AKHIOK; ZERO=0\n" + "state=AK; CITY=AKIACHAK; ZERO=0\n")
				.queryContains(mongoChecker("{$project: {CITY: '$city', STATE: '$state'}}",
						"{$sort: {STATE: 1, CITY: 1}}",
						"{$project: {STATE: 1, CITY: 1, ZERO: {$literal: 0}}}"));
	}

	@Disabled("Adhoc")
	@Test
	void testFilter() {
		assertModel(MODEL).query("select state, city from \"adhoc_schema\".\"zips\" where state = 'CA'")
				.limit(3)
				.returnsUnordered("state=CA; CITY=LOS ANGELES", "state=CA; CITY=BELL GARDENS", "state=CA; CITY=NORWALK")
				.explainContains("PLAN=MongoToEnumerableConverter\n"
						+ "  MongoProject(state=[CAST(ITEM($0, 'state')):VARCHAR(2)], CITY=[CAST(ITEM($0, 'city')):VARCHAR(20)])\n"
						+ "    MongoFilter(condition=[=(CAST(CAST(ITEM($0, 'state')):VARCHAR(2)):CHAR(2), 'CA')])\n"
						+ "      MongoTableScan(table=[[mongo_raw, zips]])");
	}

	/**
	 * MongoDB's predicates are handed (they can only accept literals on the right-hand size) so it's worth testing that
	 * we handle them right both ways around.
	 */
	@Disabled("Adhoc")
	@Test
	void testFilterReversed() {
		assertModel(MODEL)
				.query("select state, city from \"adhoc_schema\".\"zips\" where 'WI' < state order by state, city")
				.limit(3)
				.returnsOrdered("state=WV; CITY=BECKLEY", "state=WV; CITY=ELM GROVE", "state=WV; CITY=STAR CITY");

		assertModel(MODEL)
				.query("select state, city from \"adhoc_schema\".\"zips\" where state > 'WI' order by state, city")
				.limit(3)
				.returnsOrdered("state=WV; CITY=BECKLEY", "state=WV; CITY=ELM GROVE", "state=WV; CITY=STAR CITY");
	}

	/**
	 * MongoDB's predicates are handed (they can only accept literals on the right-hand size) so it's worth testing that
	 * we handle them right both ways around.
	 *
	 * <p>
	 * Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-740">[CALCITE-740] Redundant WHERE clause
	 * causes wrong result in MongoDB adapter</a>.
	 */
	@Disabled("Adhoc")
	@Test
	void testFilterPair() {
		final int gt9k = 148;
		final int lt9k = 1;
		final int gt8k = 148;
		final int lt8k = 1;
		checkPredicate(gt9k, "where pop > 8000 and pop > 9000");
		checkPredicate(gt9k, "where pop > 9000");
		checkPredicate(lt9k, "where pop < 9000");
		checkPredicate(gt8k, "where pop > 8000");
		checkPredicate(lt8k, "where pop < 8000");
		checkPredicate(gt9k, "where pop > 9000 and pop > 8000");
		checkPredicate(gt8k, "where pop > 9000 or pop > 8000");
		checkPredicate(gt8k, "where pop > 8000 or pop > 9000");
		checkPredicate(lt8k, "where pop < 8000 and pop < 9000");
	}

	private void checkPredicate(int expected, String q) {
		assertModel(MODEL).query("select count(*) as c from \"adhoc_schema\".\"zips\"\n" + q)
				.returns("C=" + expected + "\n");
		assertModel(MODEL).query("select * from \"adhoc_schema\".\"zips\"\n" + q).returnsCount(expected);
	}

	/**
	 * Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-286">[CALCITE-286] Error casting MongoDB
	 * date</a>.
	 */
	@Disabled("Adhoc")
	@Test
	void testDate() {
		assertModel(MODEL).query("select cast(_MAP['date'] as DATE) from \"mongo_raw\".\"datatypes\"")
				.returnsUnordered("EXPR$0=2012-09-05");
	}

	/**
	 * Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-5405">[CALCITE-5405] Error casting MongoDB
	 * dates to TIMESTAMP</a>.
	 */
	@Disabled("Adhoc")
	@Test
	void testDateConversion() {
		assertModel(MODEL).query("select cast(_MAP['date'] as TIMESTAMP) from \"mongo_raw\".\"datatypes\"")
				.returnsUnordered("EXPR$0=2012-09-05 00:00:00");
	}

	/**
	 * Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-5407">[CALCITE-5407] Error casting MongoDB
	 * array to VARCHAR ARRAY</a>.
	 */
	@Disabled("Adhoc")
	@Test
	void testArrayConversion() {
		assertModel(MODEL).query("select cast(_MAP['arr'] as VARCHAR ARRAY) from \"mongo_raw\".\"datatypes\"")
				.returnsUnordered("EXPR$0=[a, b]");
	}

	/**
	 * Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-665">[CALCITE-665] ClassCastException in
	 * MongoDB adapter</a>.
	 */
	@Test
	void testCountViaInt() {
		assertModel(MODEL).query("select count(*) from \"adhoc_schema\".\"zips\"").returns(input -> {
			// try {
			// assertThat(input.next(), is(true));
			// assertThat(input.getInt(1), is(ZIPS_SIZE));
			// } catch (SQLException e) {
			// throw TestUtil.rethrow(e);
			// }
		});
	}

	/**
	 * Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-6623">[CALCITE-6623] MongoDB adapter throws
	 * a java.lang.ClassCastException when Decimal128 or Binary types are used, or when a primitive value is cast to a
	 * string</a>.
	 */
	@Disabled("Adhoc")
	@Test
	void testRuntimeTypes() {
		assertModel(MODEL)
				.query("select cast(_MAP['loc'] AS varchar) " + "from \"mongo_raw\".\"zips\" where _MAP['_id']='99801'")
				.returnsCount(1)
				.returnsValue("[-134.529429, 58.362767]");

		assertModel(MODEL)
				.query("select cast(_MAP['warehouse_postal_code'] AS bigint) AS postal_code_as_bigint"
						+ " from \"mongo_raw\".\"warehouse\" where _MAP['warehouse_id']=1")
				.returnsCount(1)
				.returnsValue("55555")
				.typeIs("[POSTAL_CODE_AS_BIGINT BIGINT]");

		assertModel(MODEL)
				.query("select cast(_MAP['warehouse_postal_code'] AS varchar) AS postal_code_as_varchar"
						+ " from \"mongo_raw\".\"warehouse\" where _MAP['warehouse_id']=1")
				.returnsCount(1)
				.returnsValue("55555")
				.typeIs("[POSTAL_CODE_AS_VARCHAR VARCHAR]");

		assertModel(MODEL).query("select cast(_MAP['binaryData'] AS binary) from \"mongo_raw\".\"datatypes\"")
				.returnsCount(1)
				.returns(resultSet -> {
					try {
						resultSet.next();
						// CHECKSTYLE: IGNORE 1
						// assertThat(new String(resultSet.getBytes(1), StandardCharsets.UTF_8),
						// is("binaryData"));
					} catch (SQLException e) {
						throw TestUtil.rethrow(e);
					}
				});

		assertModel(MODEL)
				.query("select cast(_MAP['loc'] AS bigint) " + "from \"mongo_raw\".\"zips\" where _MAP['_id']='99801'")
				.throws_("Invalid field:");
	}

	/**
	 * Returns a function that checks that a particular MongoDB query has been called.
	 *
	 * @param expected
	 *            Expected query (as array)
	 * @return validation function
	 */
	private static Consumer<List> mongoChecker(final String... expected) {
		return actual -> {
			if (expected == null) {
				// assertThat("null mongo Query", actual, nullValue());
				return;
			}

			// if (expected.length == 0) {
			// CalciteAssert.assertArrayEqual("empty Mongo query", expected,
			// actual.toArray(new Object[0]));
			// return;
			// }
			//
			// // comparing list of Bsons (expected and actual)
			// final List<BsonDocument> expectedBsons =
			// Arrays.stream(expected).map(BsonDocument::parse)
			// .collect(Collectors.toList());
			//
			// final List<BsonDocument> actualBsons = ((List<?>) actual.get(0))
			// .stream()
			// .map(Objects::toString)
			// .map(BsonDocument::parse)
			// .collect(Collectors.toList());
			//
			// // compare Bson (not string) representation
			// if (!expectedBsons.equals(actualBsons)) {
			// final JsonWriterSettings settings =
			// JsonWriterSettings.builder().indent(true).build();
			// // outputs Bson in pretty Json format (with new lines)
			// // so output is human friendly in IDE diff tool
			// final Function<List<BsonDocument>, String> prettyFn = bsons -> bsons.stream()
			// .map(b -> b.toJson(settings)).collect(Collectors.joining("\n"));
			//
			// // used to pretty print Assertion error
			// assertThat("expected and actual Mongo queries (pipelines) do not match",
			// prettyFn.apply(actualBsons), is(prettyFn.apply(expectedBsons)));
			//
			// fail("Should have failed previously because expected != actual is known to be
			// true");
			// }
			return;
		};
	}

	@Disabled("Adhoc")
	@Test
	void testColumnQuoting() {
		assertModel(MODEL)
				.query("select state as \"STATE\", avg(pop) as \"AVG(pop)\" " + "from \"adhoc_schema\".\"zips\" "
						+ "group by \"STATE\" "
						+ "order by \"AVG(pop)\"")
				.limit(2)
				.returns("state=VT; AVG(pop)=26408\nstate=AK; AVG(pop)=26856\n");
	}
}