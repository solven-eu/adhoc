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
package eu.solven.adhoc.pivotable.app.example;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import eu.solven.adhoc.beta.schema.IAdhocSchema;
import eu.solven.adhoc.beta.schema.IAdhocSchemaRegistrer;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.example.worldcup.WorldCupPlayersSchema;
import eu.solven.adhoc.measure.forest.IMeasureForest;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.sql.IDSLSupplier;
import eu.solven.adhoc.table.sql.duckdb.DuckDBHelper;
import lombok.extern.slf4j.Slf4j;

/**
 * Add a simple cube for tests and demo purposes. Requires a `self` schema to be available in Spring
 * {@link ApplicationContext}.
 * 
 * @author Benoit Lacelle
 * @see TestTableQuery_DuckDb_WorldCup
 */
@Slf4j
public class InjectWorldCupExampleCubesConfig {

	// `java:S6831` as Sonar states `@Qualifier` is bad on `@Bean`
	@Profile(IPivotableSpringProfiles.P_SIMPLE_DATASETS)
	@Bean
	public Void initWorldCupCubes(@Qualifier(IPivotableSpringProfiles.P_SELF_ENDPOINT) IAdhocSchema schema) {
		log.info("Registering the {} dataset", IPivotableSpringProfiles.P_SIMPLE_DATASETS);

		registerWorldCupPlayers(schema);

		return null;
	}

	protected void registerWorldCupPlayers(IAdhocSchema schema) {
		IDSLSupplier dslSupplier = DuckDBHelper.inMemoryDSLSupplier();
		WorldCupPlayersSchema worldCupSchema = new WorldCupPlayersSchema(dslSupplier);
		ITableWrapper table = worldCupSchema.getTable(worldCupSchema.getName());
		IAdhocSchemaRegistrer registrer = schema.getRegistrer();
		registrer.registerTable(table);

		IMeasureForest forest = worldCupSchema.getForest(worldCupSchema.getName());
		registrer.registerForest(forest);
		CubeWrapper cube = worldCupSchema.makeCube(registrer, worldCupSchema, table, forest).build();
		registrer.registerCube(cube);

	}

}
