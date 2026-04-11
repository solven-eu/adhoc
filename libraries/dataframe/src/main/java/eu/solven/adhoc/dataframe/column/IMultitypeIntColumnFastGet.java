package eu.solven.adhoc.dataframe.column;

import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueReceiver;

public interface IMultitypeIntColumnFastGet extends IMultitypeColumnFastGet<Integer> {

	IValueProvider onValue(int key);

	@Override
	default IValueProvider onValue(Integer key) {
		return onValue(key.intValue());
	}

	IValueReceiver append(int key);

	@Override
	default IValueReceiver append(Integer key) {
		return append(key.intValue());
	}
}
