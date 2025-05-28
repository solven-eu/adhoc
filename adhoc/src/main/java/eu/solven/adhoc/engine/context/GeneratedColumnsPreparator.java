package eu.solven.adhoc.engine.context;

import java.util.Set;

import eu.solven.adhoc.engine.ICanResolveMeasure;
import eu.solven.adhoc.measure.MeasureForest.MeasureForestBuilder;
import eu.solven.adhoc.measure.ReferencedMeasure;
import eu.solven.adhoc.query.cube.ICubeQuery;
import lombok.Singular;
import lombok.experimental.SuperBuilder;

/**
 * This {@link IQueryPreparator} helps always keeping given measures, generating given columns. It helps 
 * 
 * @author Benoit Lacelle
 */
@SuperBuilder
@Deprecated(since = "Poor designs. Some IColumnGenerator should not come from measures")
public class GeneratedColumnsPreparator extends DefaultQueryPreparator {

	@Singular
	Set<String> generatedColumnsMeasures;

	@Override
	protected MeasureForestBuilder filterForest(ICanResolveMeasure forest, ICubeQuery preparedQuery) {
		MeasureForestBuilder filteredForest = super.filterForest(forest, preparedQuery);

		generatedColumnsMeasures.forEach(calculatedMeasure -> filteredForest
				.measure(forest.resolveIfRef(ReferencedMeasure.ref(calculatedMeasure))));

		return filteredForest;
	}

}
