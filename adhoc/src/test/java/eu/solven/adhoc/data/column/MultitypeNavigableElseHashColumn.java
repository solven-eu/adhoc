package eu.solven.adhoc.data.column;

import java.util.stream.Stream;

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.cell.IValueReceiver;
import eu.solven.adhoc.measure.transformator.iterator.SliceAndMeasure;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Deprecated(since = "Not-ready")
@Builder
@Slf4j
public class MultitypeNavigableElseHashColumn<T extends Comparable<T>> implements IMultitypeColumnFastGet<T> {
	@Default
	@NonNull
	final MultitypeNavigableColumn<T> navigable = MultitypeNavigableColumn.<T>builder().build();

	@Default
	@NonNull
	final IMultitypeColumnFastGet<T> hash = MultitypeHashColumn.<T>builder().build();

	@Override
	public long size() {
		return navigable.size() + hash.size();
	}

	@Override
	public boolean isEmpty() {
		return navigable.isEmpty() && hash.isEmpty();
	}

	@Override
	public void purgeAggregationCarriers() {
		navigable.purgeAggregationCarriers();
		hash.purgeAggregationCarriers();
	}

	@Override
	public void scan(IColumnScanner<T> rowScanner) {
		navigable.scan(rowScanner);
		hash.scan(rowScanner);
	}

	@Override
	public <U> Stream<U> stream(IColumnValueConverter<T, U> converter) {
		return Stream.concat(navigable.stream(converter), hash.stream(converter));
	}

	@Override
	public Stream<SliceAndMeasure<T>> stream() {
		return Stream.concat(navigable.stream(), hash.stream());
	}

	@Override
	public Stream<T> keyStream() {
		return Stream.concat(navigable.keyStream(), hash.keyStream());
	}

	@Override
	public IValueReceiver append(T slice) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IValueProvider onValue(T key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IValueReceiver set(T key) {
		// TODO Auto-generated method stub
		return null;
	}
}
