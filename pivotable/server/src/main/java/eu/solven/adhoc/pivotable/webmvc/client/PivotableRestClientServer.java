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
package eu.solven.adhoc.pivotable.webmvc.client;

import java.text.ParseException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpHeaders;
import org.springframework.web.client.RestClient;

import com.google.common.base.Suppliers;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import eu.solven.adhoc.beta.schema.ColumnStatistics;
import eu.solven.adhoc.beta.schema.TargetedCubeQuery;
import eu.solven.adhoc.dataframe.tabular.IReadableTabularView;
import eu.solven.adhoc.dataframe.tabular.ListBasedTabularView;
import eu.solven.adhoc.pivotable.api.IPivotableApiConstants;
import eu.solven.adhoc.pivotable.endpoint.AdhocColumnSearch;
import eu.solven.adhoc.pivotable.endpoint.AdhocCoordinatesSearch;
import eu.solven.adhoc.pivotable.endpoint.AdhocEndpointSearch;
import eu.solven.adhoc.pivotable.endpoint.PivotableAdhocEndpointMetadata;
import eu.solven.adhoc.pivotable.endpoint.TargetedEndpointSchemaMetadata;
import eu.solven.adhoc.pivotable.login.AccessTokenWrapper;
import lombok.extern.slf4j.Slf4j;

/**
 * Blocking {@link IPivotableServer} backed by Spring's {@link RestClient}. Counterpart of
 * {@link PivotableWebclientServer}, which uses WebClient and Project Reactor.
 *
 * <p>
 * The access token is fetched on first use and cached for 45 minutes via {@link Suppliers#memoizeWithExpiration}.
 *
 * @author Benoit Lacelle
 */
// https://www.baeldung.com/spring-boot-restclient
@Slf4j
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
public class PivotableRestClientServer implements IPivotableServer {

	static final String PREFIX = IPivotableApiConstants.PREFIX;

	final String baseUrl;
	final String refreshToken;

	final AtomicReference<RestClient> restClientRef = new AtomicReference<>();

	// @VisibleForTesting
	final AtomicInteger countAccessToken = new AtomicInteger();

	// Caches the access token for 45 minutes; recreated on each construction so expiry is relative to startup.
	final Supplier<String> accessTokenSupplier;

	/**
	 * Constructs a server client from a {@link PivotableWebclientServerProperties} configuration object.
	 *
	 * @param properties
	 *            the connection properties (base URL and refresh token)
	 * @return a ready-to-use {@link PivotableRestClientServer}
	 */
	public static PivotableRestClientServer fromProperties(PivotableWebclientServerProperties properties) {
		return new PivotableRestClientServer(properties.getBaseUrl(), properties.getRefreshToken());
	}

	/**
	 * @param baseUrl
	 *            the base URL of the Pivotable server (e.g. {@code https://example.com})
	 * @param refreshToken
	 *            a signed JWT refresh token used to obtain short-lived access tokens
	 */
	public PivotableRestClientServer(String baseUrl, String refreshToken) {
		this.baseUrl = baseUrl;
		this.refreshToken = refreshToken;
		// memoizeWithExpiration is thread-safe; captures this::requestAccessToken by reference.
		this.accessTokenSupplier = Suppliers.memoizeWithExpiration(() -> requestAccessToken().getAccessToken(),
				CACHE_TIMEOUT_MINUTES,
				TimeUnit.MINUTES);
		logAboutRefreshToken(refreshToken);
	}

	private void logAboutRefreshToken(String refreshToken) {
		SignedJWT jws;
		try {
			jws = SignedJWT.parse(refreshToken);
		} catch (ParseException e) {
			throw new IllegalArgumentException("Invalid refreshToken (JWS)", e);
		}

		JWTClaimsSet jwtClaimsSet;
		try {
			jwtClaimsSet = jws.getJWTClaimsSet();
		} catch (ParseException e) {
			throw new IllegalArgumentException("Invalid refreshToken (claimSet)", e);
		}

		log.info("refresh_token.jti={}", jwtClaimsSet.getJWTID());
	}

	/**
	 * Returns the shared {@link RestClient}, creating it lazily on first call.
	 *
	 * @return the configured {@link RestClient}
	 */
	protected RestClient getRestClient() {
		if (restClientRef.get() == null) {
			restClientRef.set(RestClient.builder().baseUrl(baseUrl).build());
		}
		return restClientRef.get();
	}

	private AccessTokenWrapper requestAccessToken() {
		countAccessToken.incrementAndGet();
		AccessTokenWrapper wrapper = getRestClient().get()
				.uri(uriBuilder -> uriBuilder.path(PREFIX + "/oauth2/token").build())
				.header(HttpHeaders.AUTHORIZATION, "Bearer " + refreshToken)
				.retrieve()
				.body(AccessTokenWrapper.class);
		log.info("Request for access_token: success");
		return wrapper;
	}

	/**
	 * Returns the current access token, fetching a new one from the server if the cached value has expired.
	 *
	 * @return a valid Bearer access token string
	 */
	protected String accessToken() {
		return accessTokenSupplier.get();
	}

	@Override
	public List<PivotableAdhocEndpointMetadata> searchEntrypoints(AdhocEndpointSearch search) {
		List<PivotableAdhocEndpointMetadata> result = getRestClient().get()
				.uri(uriBuilder -> uriBuilder.path(PREFIX + "/endpoints")
						.queryParamIfPresent("endpoint_id", search.getEndpointId())
						.queryParamIfPresent("keyword", search.getKeyword())
						.build())
				.header(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken())
				.retrieve()
				// List<PivotableAdhocEndpointMetadata>
				.body(new ParameterizedTypeReference<>() {
				});
		log.info("Search for endpoints: {}", result);
		return result;
	}

	@Override
	public List<TargetedEndpointSchemaMetadata> searchSchemas(AdhocEndpointSearch search) {
		List<TargetedEndpointSchemaMetadata> result = getRestClient().get()
				.uri(uriBuilder -> uriBuilder.path(PREFIX + "/endpoints/schemas")
						.queryParamIfPresent("endpoint_id", search.getEndpointId())
						.queryParamIfPresent("keyword", search.getKeyword())
						.build())
				.header(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken())
				.retrieve()
				// List<TargetedEndpointSchemaMetadata>
				.body(new ParameterizedTypeReference<>() {
				});
		log.info("Search for schemas: {}", result);
		return result;
	}

	@Override
	public List<ColumnStatistics> columnMetadata(AdhocColumnSearch search) {
		List<ColumnStatistics> result = getRestClient().get()
				.uri(uriBuilder -> uriBuilder.path(PREFIX + "/endpoints/schemas/columns")
						.queryParamIfPresent("endpoint_id", search.getEndpointId())
						.queryParamIfPresent("cube", search.getCube())
						.queryParamIfPresent("table", search.getTable())
						.queryParamIfPresent("name", search.getName())
						.queryParamIfPresent("coordinate", search.getCoordinate())
						.build())
				.header(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken())
				.retrieve()
				// List<ColumnStatistics>
				.body(new ParameterizedTypeReference<>() {
				});
		log.info("Search for columns: {}", result);
		return result;
	}

	@Override
	public List<ColumnStatistics> searchMembers(AdhocCoordinatesSearch search) {
		// TODO Not yet implemented
		throw new UnsupportedOperationException("searchMembers is not yet implemented");
	}

	@Override
	public IReadableTabularView executeQuery(TargetedCubeQuery query) {
		IReadableTabularView result = getRestClient().post()
				.uri(uriBuilder -> uriBuilder.path(PREFIX + "/cubes/query").build())
				.header(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken())
				.body(query)
				.retrieve()
				.body(ListBasedTabularView.class);
		log.info("Execute a query: success");
		return result;
	}

	/**
	 * @return the number of access-token requests made to the server since this instance was created
	 */
	public int getNbAccessTokens() {
		return countAccessToken.get();
	}
}
