package eu.solven.adhoc.measure.transformator;

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.cell.IValueReceiver;
import eu.solven.adhoc.measure.combination.ICombination;

/**
 * Enables processing an {@link ICombination} along columns, without having to create {@link IValueReceiver} for each
 * row.
 * 
 * @author Benoit Lacelle
 */
@Deprecated(since = "Not-Ready")
public interface ICombinationBinding {

	ICombinationBinding NULL = null;

	static ICombinationBinding return0() {
		// TODO Auto-generated method stub
		return null;
	}

	public static interface On2 {
		void on3(IValueReceiver valueReceiver, IValueProvider left, IValueProvider right);
	}

	static ICombinationBinding on2(On2 on3) {
		return null;
	}

	interface IRowState {
		void receive(int index, IValueProvider valueProvider);

		IValueReceiver receive(int index);
	}

}
