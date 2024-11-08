package eu.solven.adhoc.storage;

public interface ValueConsumer {
	void onDouble(double d);

	void onLong(long l);

	void onCharsequence(CharSequence charSequence);

	void onObject(Object object);
}
