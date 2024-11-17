package eu.solven.adhoc.api.v1.pojo;

import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.filters.INotFilter;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Value
@Builder
@Jacksonized
public class NotFilter implements INotFilter {

	@NonNull
	final IAdhocFilter negated;

	@Override
	public boolean isNot() {
		return true;
	}

}