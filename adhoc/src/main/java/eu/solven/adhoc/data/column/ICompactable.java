package eu.solven.adhoc.data.column;

/**
 * Used for data structure which can be compacted. Typically useful when all write operations are done, and we want to
 * minimize memory consumption.
 * 
 * @author Benoit Lacelle
 */
public interface ICompactable {
	void compact();
}
