package eu.solven.adhoc.map.factory;

import java.util.Set;
import java.util.function.IntFunction;

import eu.solven.adhoc.map.IAdhocMap;
import lombok.Builder;
import lombok.NonNull;

/**
 * Represents an {@link IAdhocMap} given an {@link IntFunction} representation dictionarized value.
 * 
 * @author Benoit Lacelle
 * 
 */
public class MapOverIntFunction extends AbstractAdhocMap {

	@NonNull
	final IntFunction<Object> sequencedValues;

	@Builder
	public MapOverIntFunction(ISliceFactory factory, SequencedSetLikeList keys, IntFunction<Object> unorderedValues) {
		super(factory, keys);
		this.sequencedValues = unorderedValues;
	}

	@Override
	protected Object getSequencedValueRaw(int index) {
		return sequencedValues.apply(index);
	}

	@Override
	protected Object getSortedValueRaw(int index) {
		return getSequencedValueRaw(sequencedKeys.unorderedIndex(index));
	}

	@Override
	public IAdhocMap retainAll(Set<String> retainedColumns) {
		RetainedKeySet retainedKeyset = retainKeyset(retainedColumns);

		int[] sequencedIndexes = retainedKeyset.getSequencedIndexes();
		IntFunction<Object> retainedSequencedValues = index -> sequencedValues.apply(sequencedIndexes[index]);

		return new MapOverIntFunction(getFactory(), retainedKeyset.getKeys(), retainedSequencedValues);
	}
}