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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.jooq.impl.DSL;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import eu.solven.adhoc.beta.schema.AdhocSchema;
import eu.solven.adhoc.measure.MeasureForest;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapperParameters;
import eu.solven.adhoc.table.sql.duckdb.DuckDbHelper;
import lombok.extern.slf4j.Slf4j;

/**
 * Add a simple cube for tests and demo purposes. Requires a `self` schema to be available in Spring
 * {@link ApplicationContext}.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
public class InjectAdvancedCubesConfig {

	@Profile(IPivotableSpringProfiles.P_ADVANCED_DATASETS)
	@Bean
	public Void initAdvancedCubes(@Qualifier(IPivotableSpringProfiles.P_SELF_ENDPOINT) AdhocSchema schema) {
		log.info("Registering the {} dataset", IPivotableSpringProfiles.P_SIMPLE_DATASETS);

		registerBan(schema);

		return null;
	}

	// https://www.data.gouv.fr/fr/datasets/ban-format-parquet/
	protected void registerBan(AdhocSchema schema) {
		Path pathToParquet = Path.of("/Users/blacelle/Downloads/datasets/adresses-france-10-2024.parquet");

		if (!Files.isReadable(pathToParquet)) {
			log.warn("path=`{}` is not readable. The file probably does not exist.", pathToParquet);
		}
		JooqTableWrapper table = new JooqTableWrapper("ban",
				JooqTableWrapperParameters.builder()
						.dslSupplier(DuckDbHelper.inMemoryDSLSupplier())
						.table(DSL.table("'%s'".formatted(pathToParquet)))
						.build());

		schema.registerTable(table);

		List<IMeasure> measures = new ArrayList<>();

		measures.add(Aggregator.countAsterisk());

		schema.registerForest(MeasureForest.fromMeasures("ban", measures));

		schema.registerCube("ban", "ban", "ban");
	}

}
