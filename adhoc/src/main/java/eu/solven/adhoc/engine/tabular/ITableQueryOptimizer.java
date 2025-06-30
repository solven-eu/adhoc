package eu.solven.adhoc.engine.tabular;

import java.util.Map;
import java.util.Set;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import eu.solven.adhoc.data.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.query.cube.IHasQueryOptions;
import eu.solven.adhoc.query.table.TableQuery;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;

public interface ITableQueryOptimizer {

	@Value
	@Builder
	class SplitTableQueries {
		// Holds the TableQuery which can not be implicitly evaluated, and needs to be executed directly
		@Singular
		@NonNull
		Set<CubeQueryStep> inducers;

		// Holds the TableQuery which can be evaluated implicitly from underlyings
		@Singular
		@NonNull
		Set<CubeQueryStep> induceds;

		@NonNull
		DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> tableQueriesDag;

		public static SplitTableQueries empty() {
			return SplitTableQueries.builder()
					.induceds(Set.of())
					.inducers(Set.of())
					.tableQueriesDag(new DirectedAcyclicGraph<>(DefaultEdge.class))
					.build();
		}
	}

	/**
	 * 
	 * @param tableQueries
	 * @return an Object partitioning TableQuery which can not be induced from those which can be induced.
	 */
	SplitTableQueries splitInduced(IHasQueryOptions hasOptions, Set<TableQuery> tableQueries);

	IMultitypeMergeableColumn<SliceAsMap> evaluateInduced(AdhocFactories factories,
			IHasQueryOptions hasOptions,
			SplitTableQueries inducerAndInduced,
			Map<CubeQueryStep, ISliceToValue> stepToValues,
			CubeQueryStep induced);
}
