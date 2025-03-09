package eu.solven.adhoc.storage;

import java.util.concurrent.atomic.AtomicReference;

/**
 * While {@link IValueConsumer} can be interpreted as a way to transmit data, {@link IValueProvider} is a way to
 * transmit data in the opposite direction.
 * <p>
 * Typically, a class would provide a {@link IValueConsumer} to receive data/to be written into. Then, a
 * {@link IValueProvider} can be seen as a way to send data/to be read from.
 */
public interface IValueProvider {
	void acceptConsumer(IValueConsumer valueConsumer);

	static Object getValue(IValueProvider valueProvider) {
		AtomicReference<Object> refV = new AtomicReference<>();

		valueProvider.acceptConsumer(refV::set);

		return refV.get();
	}
}
