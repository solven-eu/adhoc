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
package eu.solven.adhoc.pivotable.client;

import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import eu.solven.adhoc.pivotable.account.fake_user.FakeUser;
import eu.solven.adhoc.pivotable.account.fake_user.RandomUser;
import eu.solven.adhoc.pivotable.login.RefreshTokenWrapper;
import eu.solven.adhoc.pivotable.oauth2.authorizationserver.PivotableTokenService;
import eu.solven.adhoc.tools.IUuidGenerator;
import eu.solven.adhoc.tools.JdkUuidGenerator;
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Value
@Builder
@Slf4j
public class PivotableWebclientServerProperties {
	public static final String KEY_PLAYER_CONTESTBASEURL = "adhoc.pivotable.player.contest-base-url";

	public static final String ENV_REFRESH_TOKEN = "adhoc.pivotable.player.refresh_token";
	public static final String PLACEHOLDER_GENERATEFAKEPLAYER = "GENERATE_FAKEUSER";
	public static final String PLACEHOLDER_GENERATERANDOMPLAYER = "GENERATE_RANDOMUSER";

	String baseUrl;
	String refreshToken;

	public static String loadRefreshToken(Environment env, IUuidGenerator uuidGenerator, String refreshToken) {
		if ("NEEDS_A_PROPER_VALUE".equals(refreshToken)) {
			throw new IllegalStateException(
					"Needs to define properly '%s'".formatted(PivotableWebclientServerProperties.ENV_REFRESH_TOKEN));
		} else if (PivotableWebclientServerProperties.PLACEHOLDER_GENERATEFAKEPLAYER.equals(refreshToken)) {
			if (!env.acceptsProfiles(Profiles.of(IPivotableSpringProfiles.P_FAKEUSER))) {
				throw new IllegalStateException(
						"Can not generate a refreshToken if not `%s`".formatted(IPivotableSpringProfiles.P_FAKEUSER));
			} else {
				log.info("Generating on-the-fly a fakeUser refreshToken");
			}
			PivotableTokenService kumiteTokenService = new PivotableTokenService(env, uuidGenerator);
			RefreshTokenWrapper wrappedRefreshToken = kumiteTokenService.wrapInJwtRefreshToken(FakeUser.ACCOUNT_ID);
			refreshToken = wrappedRefreshToken.getRefreshToken();
		} else if (PivotableWebclientServerProperties.PLACEHOLDER_GENERATERANDOMPLAYER.equals(refreshToken)) {
			{
				log.info("Generating on-the-fly a fakeUser refreshToken");
			}
			PivotableTokenService kumiteTokenService = new PivotableTokenService(env, uuidGenerator);
			RefreshTokenWrapper wrappedRefreshToken = kumiteTokenService.wrapInJwtRefreshToken(RandomUser.ACCOUNT_ID);
			refreshToken = wrappedRefreshToken.getRefreshToken();
		}
		return refreshToken;
	}

	public static PivotableWebclientServerProperties forTests(Environment env, int randomServerPort) {
		String refreshToken = loadRefreshToken(env,
				JdkUuidGenerator.INSTANCE,
				PivotableWebclientServerProperties.PLACEHOLDER_GENERATERANDOMPLAYER);

		// https://github.com/spring-projects/spring-boot/issues/5077
		String baseUrl = env.getRequiredProperty(KEY_PLAYER_CONTESTBASEURL)
				.replaceFirst("LocalServerPort", Integer.toString(randomServerPort));

		return PivotableWebclientServerProperties.builder().baseUrl(baseUrl).refreshToken(refreshToken).build();
	}
}
