package eu.solven.adhoc.util;

import java.util.function.Supplier;

import com.google.common.base.Suppliers;

import eu.solven.adhoc.data.cell.IValueReceiver;

/**
 * Centralizes all implementations doing nothing given input data.
 * 
 * @author Benoit Lacelle
 */
public class AdhocBlackHole implements IValueReceiver, IAdhocEventBus {
	private static final Supplier<AdhocBlackHole> MEMOIZED = Suppliers.memoize(() -> new AdhocBlackHole());

	public static AdhocBlackHole getInstance() {
		return MEMOIZED.get();
	}

	@Override
	public void onLong(long v) {
		// do nothing with the value
	}

	@Override
	public void onDouble(double v) {
		// do nothing with the value
	}

	@Override
	public void onObject(Object v) {
		// do nothing with the value
	}

	@Override
	public void post(Object event) {
		// do nothing with given event
	}
}
