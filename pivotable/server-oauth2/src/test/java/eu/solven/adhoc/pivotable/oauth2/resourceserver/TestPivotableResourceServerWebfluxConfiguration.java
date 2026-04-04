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
package eu.solven.adhoc.pivotable.oauth2.resourceserver;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder;

import eu.solven.adhoc.pivotable.oauth2.IPivotableOAuth2Constants;
import eu.solven.adhoc.tools.JdkUuidGenerator;

/**
 * Tests for {@link PivotableResourceServerWebfluxConfiguration}.
 *
 * @author Benoit Lacelle
 */
public class TestPivotableResourceServerWebfluxConfiguration {

	final PivotableResourceServerWebfluxConfiguration config = new PivotableResourceServerWebfluxConfiguration();

	/**
	 * {@link PivotableResourceServerWebfluxConfiguration#reactiveJwtDecoder} must produce a non-null
	 * {@link ReactiveJwtDecoder} when a generated signing key is configured.
	 */
	@Test
	public void testReactiveJwtDecoder_notNull() {
		MockEnvironment env = new MockEnvironment();
		env.setProperty(IPivotableOAuth2Constants.KEY_JWT_SIGNINGKEY, IPivotableOAuth2Constants.GENERATE);

		ReactiveJwtDecoder decoder = config.reactiveJwtDecoder(env, JdkUuidGenerator.INSTANCE);

		Assertions.assertThat(decoder).isNotNull();
	}

	/**
	 * Calling {@link PivotableResourceServerWebfluxConfiguration#reactiveJwtDecoder} twice with the same environment
	 * must produce two non-null decoders (key stability is already verified by
	 * {@link TestPivotableResourceServerConfiguration#testGenerateMultipleTimes}).
	 */
	@Test
	public void testReactiveJwtDecoder_stableKeyAcrossCalls() {
		MockEnvironment env = new MockEnvironment();
		env.setProperty(IPivotableOAuth2Constants.KEY_JWT_SIGNINGKEY, IPivotableOAuth2Constants.GENERATE);

		ReactiveJwtDecoder decoder1 = config.reactiveJwtDecoder(env, JdkUuidGenerator.INSTANCE);
		ReactiveJwtDecoder decoder2 = config.reactiveJwtDecoder(env, JdkUuidGenerator.INSTANCE);

		Assertions.assertThat(decoder1).isNotNull();
		Assertions.assertThat(decoder2).isNotNull();
	}
}
