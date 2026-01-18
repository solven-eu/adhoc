package eu.solven.adhoc.compression.column;

import eu.solven.adhoc.compression.page.IReadableColumn;

public interface IFreezingStrategy {
	IReadableColumn freeze(IAppendableColumn column);
}
