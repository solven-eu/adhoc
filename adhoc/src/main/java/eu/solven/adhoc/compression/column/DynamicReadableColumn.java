package eu.solven.adhoc.compression.column;

import eu.solven.adhoc.compression.page.IReadableColumn;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Enable an {@link IAppendableColumn} to be switched to another instance dynamically. Typically used by asynchronous {@link eu.solven.adhoc.compression.column.freezer.IFreezingStrategy}.
 *
 * @author Benoit Lacelle
 */
public class DynamicReadableColumn implements IReadableColumn{

    final AtomicReference<IReadableColumn> ref = new AtomicReference<>();

    public DynamicReadableColumn(IReadableColumn initialColumn) {
        ref.set(initialColumn);
    }

    @Override
    public Object readValue(int rowIndex) {
        return ref.get().readValue(rowIndex);
    }

    public void setRef(IReadableColumn frozen) {
        ref.set(frozen);
    }
}
