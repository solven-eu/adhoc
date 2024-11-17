package eu.solven.adhoc.transformers;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import eu.solven.adhoc.aggregations.ICombination;
import eu.solven.adhoc.aggregations.IOperatorsFactory;
import eu.solven.adhoc.coordinate.NavigableMapComparator;
import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.dag.CoordinatesToValues;
import eu.solven.adhoc.storage.AsObjectValueConsumer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class CombinatorQueryStep implements IHasUnderlyingQuerySteps {
	final Combinator combinator;
	final IOperatorsFactory transformationFactory;
	final AdhocQueryStep step;

	public List<String> getUnderlyingNames() {
		return combinator.getUnderlyingNames();
	};

	@Override
	public List<AdhocQueryStep> getUnderlyingSteps() {
		return getUnderlyingNames().stream().map(underlyingName -> {
			return AdhocQueryStep.builder()
					.filter(step.getFilter())
					.groupBy(step.getGroupBy())
					.measure(ReferencedMeasure.builder().ref(underlyingName).build())
					.build();
		}).toList();
	}

	@Override
	public CoordinatesToValues produceOutputColumn(List<CoordinatesToValues> underlyings) {
		if (underlyings.size() != getUnderlyingNames().size()) {
			throw new IllegalArgumentException("underlyingNames.size() != underlyings.size()");
		} else if (underlyings.isEmpty()) {
			return CoordinatesToValues.empty();
		}

		CoordinatesToValues output = CoordinatesToValues.builder().build();

		ICombination tranformation = transformationFactory.makeTransformation(combinator);

		for (Map<String, ?> coordinate : BucketorQueryStep.keySet(combinator.isDebug(), underlyings)) {
			List<Object> underlyingVs = underlyings.stream().map(storage -> {
				AtomicReference<Object> refV = new AtomicReference<>();
				AsObjectValueConsumer consumer = AsObjectValueConsumer.consumer(o -> {
					refV.set(o);
				});

				storage.onValue(coordinate, consumer);

				return refV.get();
			}).collect(Collectors.toList());

			Object value = tranformation.combine(underlyingVs);
			output.put(coordinate, value);
		}

		return output;
	}

	public static Iterable<? extends Map<String, ?>> keySet(boolean debug, List<CoordinatesToValues> underlyings) {
		Set<Map<String, ?>> keySet;
		if (debug) {
			// Enforce an iteration order for debugging-purposes
			Comparator<Map<String, ?>> mapComparator =
					Comparator.<Map<String, ?>, NavigableMap<String, ?>>comparing(s -> new TreeMap<String, Object>(s),
							NavigableMapComparator.INSTANCE);
			keySet = new TreeSet<>(mapComparator);
		} else {
			keySet = new HashSet<>();
		}

		for (CoordinatesToValues underlying : underlyings) {
			keySet.addAll(underlying.getStorage().keySet());
		}

		return keySet;
	}
}
