package eu.solven.adhoc.database;

import lombok.NonNull;

/**
 * Sometimes (e.g. in early projects) there is a direct mapping from columns used by {@link eu.solven.adhoc.query.AdhocQuery} and those provided by a {@link IAdhocDatabaseWrapper}. Then, the transcoding is the identity.
 */
public class IdentityTranscoder implements IAdhocDatabaseTranscoder {
    @Override
    public String underlying(String queried) {
        return queried;
    }

    @Override
    public String queried(String underlying) {
        return underlying;
    }
}
