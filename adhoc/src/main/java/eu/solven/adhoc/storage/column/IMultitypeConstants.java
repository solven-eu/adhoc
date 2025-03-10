package eu.solven.adhoc.storage.column;

/**
 * Constants related to multitype data-structures.
 */
public interface IMultitypeConstants {
    byte MASK_EMPTY = 0;
    byte MASK_LONG = 1;
    byte MASK_DOUBLE = 1 << 1;
    byte MASK_VARCHAR = 1 << 2;
    byte MASK_OBJECT = 1 << 3;
}
