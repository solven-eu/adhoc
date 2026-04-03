package eu.solven.adhoc.pivotable.webnone.security.oauth2;

import eu.solven.adhoc.pivotable.account.PivotableUsersRegistry;
import eu.solven.adhoc.pivotable.account.internal.PivotableUser;
import eu.solven.adhoc.pivotable.account.internal.PivotableUserPreRegister;

/**
 * Helps registering a user with {@link PivotableUsersRegistry}.
 * 
 * @author Benoit Lacelle
 */
public interface IOnPivotableUser {

	PivotableUser onAdhocUserRaw(PivotableUserPreRegister userPreRegister);
}
