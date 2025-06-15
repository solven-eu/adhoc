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
import eu.solven.adhoc.beta.schema.AdhocSchema;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.engine.context.GeneratedColumnsPreparator;
import eu.solven.adhoc.example.worldcup.WorldCupPlayersSchema;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.table.ITableWrapper;
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
	public Void initWorldCupCubes(@Qualifier(IPivotableSpringProfiles.P_SELF_ENDPOINT) AdhocSchema schema) {
		log.info("Registering the {} dataset", IPivotableSpringProfiles.P_SIMPLE_DATASETS);

		registerWorldCupPlayers(schema);

		return null;
	}

	protected void registerWorldCupPlayers(AdhocSchema schema) {
		WorldCupPlayersSchema worldCupSchema = new WorldCupPlayersSchema();
		ITableWrapper table = worldCupSchema.getTable(worldCupSchema.getName());
		schema.registerTable(table);

		IMeasureForest forest = worldCupSchema.getForest(worldCupSchema.getName());
		schema.registerForest(forest);
		CubeWrapper cube = schema.openCubeWrapperBuilder()
				.name(worldCupSchema.getName())
				.forest(forest)
				.table(table)
				.queryPreparator(GeneratedColumnsPreparator.builder().generatedColumnsMeasure("event_count").build())
				.build();
		schema.registerCube(cube);

	}

}
