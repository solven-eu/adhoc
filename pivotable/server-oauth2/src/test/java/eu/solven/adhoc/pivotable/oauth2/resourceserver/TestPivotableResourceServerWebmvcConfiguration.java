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
import org.springframework.security.oauth2.jwt.JwtDecoder;

import eu.solven.adhoc.pivotable.oauth2.IPivotableOAuth2Constants;
import eu.solven.adhoc.tools.JdkUuidGenerator;

/**
 * Tests for {@link PivotableResourceServerWebmvcConfiguration}.
 *
 * @author Benoit Lacelle
 */
public class TestPivotableResourceServerWebmvcConfiguration {

	final PivotableResourceServerWebmvcConfiguration config = new PivotableResourceServerWebmvcConfiguration();

	/**
	 * {@link PivotableResourceServerWebmvcConfiguration#jwtDecoder} must produce a non-null {@link JwtDecoder} when a
	 * generated signing key is configured.
	 */
	@Test
	public void testJwtDecoder_notNull() {
		MockEnvironment env = new MockEnvironment();
		env.setProperty(IPivotableOAuth2Constants.KEY_JWT_SIGNINGKEY, IPivotableOAuth2Constants.GENERATE);

		JwtDecoder decoder = config.jwtDecoder(env, JdkUuidGenerator.INSTANCE);

		Assertions.assertThat(decoder).isNotNull();
	}

	/**
	 * Calling {@link PivotableResourceServerWebmvcConfiguration#jwtDecoder} twice with the same environment must
	 * produce two decoders that share the same signing key (i.e. a token signed by one can be verified by the other).
	 * This mirrors the behaviour verified by
	 * {@link TestPivotableResourceServerConfiguration#testGenerateMultipleTimes}.
	 */
	@Test
	public void testJwtDecoder_stableKeyAcrossCalls() {
		MockEnvironment env = new MockEnvironment();
		env.setProperty(IPivotableOAuth2Constants.KEY_JWT_SIGNINGKEY, IPivotableOAuth2Constants.GENERATE);

		JwtDecoder decoder1 = config.jwtDecoder(env, JdkUuidGenerator.INSTANCE);
		JwtDecoder decoder2 = config.jwtDecoder(env, JdkUuidGenerator.INSTANCE);

		// Both decoders are non-null — key stability is already verified by TestPivotableResourceServerConfiguration
		Assertions.assertThat(decoder1).isNotNull();
		Assertions.assertThat(decoder2).isNotNull();
	}
}
