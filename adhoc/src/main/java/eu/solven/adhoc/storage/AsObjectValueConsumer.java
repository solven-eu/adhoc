package eu.solven.adhoc.storage;

import java.util.function.Consumer;

import lombok.Value;

/**
 * Simple {@link ValueConsumer} treating all inputs as Objects
 * 
 * @author Benoit Lacelle
 *
 */
@Value
public class AsObjectValueConsumer implements ValueConsumer {

	final Consumer<Object> consumer;

	@Override
	public void onDouble(double d) {
		consumer.accept(d);
	}

	@Override
	public void onLong(long l) {
		consumer.accept(l);
	}

	@Override
	public void onCharsequence(CharSequence charSequence) {
		// Turn to String for simplification
		// If performance is a matter, do not user AsObjectValueConsumer at all
		consumer.accept(charSequence.toString());
	}

	@Override
	public void onObject(Object object) {
		consumer.accept(object);
	}

	public static AsObjectValueConsumer consumer(Consumer<Object> consumer) {
		return new AsObjectValueConsumer(consumer);
	}
}
