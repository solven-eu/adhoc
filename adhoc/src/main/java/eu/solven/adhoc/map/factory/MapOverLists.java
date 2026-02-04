package eu.solven.adhoc.map.factory;

import java.util.AbstractList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.map.IAdhocMap;
import lombok.Builder;
import lombok.NonNull;

/**
 * A {@link Map} based on {@link SequencedSetLikeList} and values as a {@link List}.
 *
 * @author Benoit Lacelle
 */
public class MapOverLists extends AbstractAdhocMap {

	@NonNull
	final List<?> sequencedValues;

	@Builder
	public MapOverLists(ISliceFactory factory, SequencedSetLikeList keys, ImmutableList<?> sequencedValues) {
		super(factory, keys);
		this.sequencedValues = sequencedValues;
	}

	protected MapOverLists(ISliceFactory factory, SequencedSetLikeList keys, List<?> sequencedValues) {
		super(factory, keys);
		this.sequencedValues = sequencedValues;
	}

	@Override
	protected Object getSequencedValueRaw(int index) {
		return sequencedValues.get(index);
	}

	@Override
	protected Object getSortedValueRaw(int index) {
		return sequencedValues.get(sequencedKeys.unorderedIndex(index));
	}

	@Override
	public IAdhocMap retainAll(Set<String> retainedColumns) {
		RetainedKeySet retainedKeyset = retainKeyset(retainedColumns);

		int[] retainedIndexes = retainedKeyset.getSequencedIndexes();
		List<?> retainedSequencedValues = new AbstractList<>() {

			@Override
			public int size() {
				return retainedColumns.size();
			}

			@Override
			public Object get(int index) {
				int originalIndex = retainedIndexes[index];
				if (originalIndex == -1) {
					// retained a not present column
					return null;
				} else {
					return sequencedValues.get(originalIndex);
				}
			}
		};

		return new MapOverLists(factory, retainedKeyset.getKeys(), retainedSequencedValues);
	}
}