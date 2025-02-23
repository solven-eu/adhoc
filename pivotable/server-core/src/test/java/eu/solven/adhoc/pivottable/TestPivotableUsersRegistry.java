/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package eu.solven.adhoc.pivottable;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import eu.solven.adhoc.pivotable.account.PivotableUsersRegistry;
import eu.solven.adhoc.pivotable.account.internal.PivotableUser;
import eu.solven.adhoc.pivotable.account.internal.PivotableUserPreRegister;
import eu.solven.adhoc.pivotable.account.login.IPivotableTestConstants;
import eu.solven.adhoc.pivotable.app.persistence.InMemoryPivotableConfiguration;
import eu.solven.adhoc.tools.JdkUuidGenerator;
import lombok.extern.slf4j.Slf4j;

@ExtendWith(SpringExtension.class)
@Import({ JdkUuidGenerator.class,

		InMemoryPivotableConfiguration.class,

		PivotableUsersRegistry.class, })

@ActiveProfiles({ IPivotableSpringProfiles.P_INMEMORY })
@Slf4j
public class TestPivotableUsersRegistry implements IPivotableTestConstants {
	@Autowired
	PivotableUsersRegistry usersRegistry;

	@Test
	public void testUnknownUser() {
		Assertions.assertThatThrownBy(() -> usersRegistry.getUser(someAccountId))
				.isInstanceOf(IllegalArgumentException.class);
	}

	@Test
	public void testRegisterUser() {
		PivotableUserPreRegister raw = IPivotableTestConstants.userPreRegister();
		PivotableUser registered = usersRegistry.registerOrUpdate(raw);

		Assertions.assertThat(usersRegistry.getUser(registered.getAccountId())).isEqualTo(registered);
	}
}
