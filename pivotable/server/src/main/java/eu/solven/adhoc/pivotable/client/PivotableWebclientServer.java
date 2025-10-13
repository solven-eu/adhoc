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

import java.text.ParseException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClient.RequestHeadersSpec;

import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import eu.solven.adhoc.beta.schema.ColumnStatistics;
import eu.solven.adhoc.beta.schema.TargetedCubeQuery;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.pivotable.endpoint.AdhocColumnSearch;
import eu.solven.adhoc.pivotable.endpoint.AdhocCoordinatesSearch;
import eu.solven.adhoc.pivotable.endpoint.AdhocEndpointSearch;
import eu.solven.adhoc.pivotable.endpoint.PivotableAdhocEndpointMetadata;
import eu.solven.adhoc.pivotable.endpoint.TargetedEndpointSchemaMetadata;
import eu.solven.adhoc.pivotable.login.AccessTokenWrapper;
import eu.solven.adhoc.pivotable.webflux.api.PivotableApiRouter;
import eu.solven.adhoc.pivottable.api.IPivotableApiConstants;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * See routes as defined in {@link PivotableApiRouter}.
 * 
 * @author Benoit Lacelle
 *
 */
// https://www.baeldung.com/spring-5-webclient
@Slf4j
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
public class PivotableWebclientServer implements IPivotableServer {
	static final String PREFIX = IPivotableApiConstants.PREFIX;

	final String baseUrl;
	final String refreshToken;

	final AtomicReference<WebClient> webClientRef = new AtomicReference<>();

	// @VisibleForTesting
	final AtomicInteger countAccessToken = new AtomicInteger();

	final AtomicReference<Mono<AccessTokenWrapper>> accessTokenSupplier = new AtomicReference<>();

	public static PivotableWebclientServer fromProperties(PivotableWebclientServerProperties properties) {
		return new PivotableWebclientServer(properties.getBaseUrl(), properties.getRefreshToken());
	}

	public PivotableWebclientServer(String baseUrl, String refreshToken) {
		this.baseUrl = baseUrl;
		this.refreshToken = refreshToken;

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

	WebClient getWebClient() {
		if (webClientRef.get() == null) {
			webClientRef.set(WebClient.builder()
					.baseUrl(baseUrl)
					// TODO Generate accessToken given the refreshToken
					// .defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + refreshToken)
					.build());
		}

		return webClientRef.get();
	}

	private Mono<AccessTokenWrapper> requestAccessToken() {
		RequestHeadersSpec<?> spec = getWebClient().get()
				.uri(uriBuilder -> uriBuilder.path(PREFIX + "/oauth2/token")
						// .queryParam("player_id", playerId.toString())
						.build())
				.header(HttpHeaders.AUTHORIZATION, "Bearer " + refreshToken);

		return spec.exchangeToMono(r -> {
			countAccessToken.incrementAndGet();

			if (!r.statusCode().is2xxSuccessful()) {
				throw new IllegalArgumentException("Request rejected: " + r.statusCode());
			}
			log.info("Request for access_token: {}", r.statusCode());
			return r.bodyToMono(AccessTokenWrapper.class);
		});
	}

	// https://stackoverflow.com/questions/65972564/spring-reactive-web-client-rest-request-with-oauth-token-in-case-of-401-response
	@SuppressWarnings("checkstyle:MagicNumber")
	Mono<AccessTokenWrapper> accessToken() {
		// Return the same Mono for 45 minutes
		return requestAccessToken()
				// Given Mono will see its value cache for 45 minutes
				// BEWARE This will not automatically discard the cached value if we get (for any reason) a 401
				.cache(Duration.ofMinutes(45));
	}

	// see PivotableEntrypointsHandler
	@Override
	public Flux<PivotableAdhocEndpointMetadata> searchEntrypoints(AdhocEndpointSearch search) {
		return accessToken().map(accessToken -> {
			return getWebClient().get()
					.uri(uriBuilder -> uriBuilder.path(PREFIX + "/endpoints")
							.queryParamIfPresent("endpoint_id", search.getEndpointId())
							.queryParamIfPresent("keyword", search.getKeyword())
							.build())
					.header(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken.getAccessToken());
		}).flatMapMany(spec -> {
			return spec.exchangeToFlux(r -> {
				if (!r.statusCode().is2xxSuccessful()) {
					throw new IllegalArgumentException("Request rejected: " + r.statusCode());
				}
				log.info("Search for endpoints: {}", r.statusCode());
				return r.bodyToFlux(PivotableAdhocEndpointMetadata.class);
			});
		});
	}

	@Override
	public Flux<TargetedEndpointSchemaMetadata> searchSchemas(AdhocEndpointSearch search) {
		return accessToken().map(accessToken -> {
			return getWebClient().get()
					.uri(uriBuilder -> uriBuilder.path(PREFIX + "/endpoints/schemas")
							.queryParamIfPresent("endpoint_id", search.getEndpointId())
							.queryParamIfPresent("keyword", search.getKeyword())
							.build())
					.header(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken.getAccessToken());
		}).flatMapMany(spec -> {
			return spec.exchangeToFlux(r -> {
				if (!r.statusCode().is2xxSuccessful()) {
					throw new IllegalArgumentException("Request rejected: " + r.statusCode());
				}
				log.info("Search for schemas: {}", r.statusCode());
				return r.bodyToFlux(TargetedEndpointSchemaMetadata.class);
			});
		});
	}

	@Override
	public Flux<ColumnStatistics> columnMetadata(AdhocColumnSearch search) {
		return accessToken().map(accessToken -> {
			return getWebClient().get()
					.uri(uriBuilder -> uriBuilder.path(PREFIX + "/endpoints/schemas/columns")
							.queryParamIfPresent("endpoint_id", search.getEndpointId())
							.queryParamIfPresent("cube", search.getCube())
							.queryParamIfPresent("table", search.getTable())
							.queryParamIfPresent("name", search.getName())
							.queryParamIfPresent("coordinate", search.getCoordinate())
							.build())
					.header(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken.getAccessToken());
		}).flatMapMany(spec -> {
			return spec.exchangeToFlux(r -> {
				if (!r.statusCode().is2xxSuccessful()) {
					throw new IllegalArgumentException("Request rejected: " + r.statusCode());
				}
				log.info("Search for columns: {}", r.statusCode());
				return r.bodyToFlux(ColumnStatistics.class);
			});
		});
	}

	@Override
	public Flux<ColumnStatistics> searchMembers(AdhocCoordinatesSearch search) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Mono<ITabularView> executeQuery(TargetedCubeQuery query) {
		return accessToken().map(accessToken -> {
			return getWebClient().post().uri(uriBuilder -> {
				return uriBuilder.path(PREFIX + "/cubes/query").build();
			}).header(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken.getAccessToken());
		}).flatMap(spec -> {
			return spec.bodyValue(query).exchangeToMono(r -> {
				if (!r.statusCode().is2xxSuccessful()) {
					throw new IllegalArgumentException("Request rejected: " + r.statusCode());
				}
				log.info("Execute a query: {}", r.statusCode());
				return r.bodyToMono(ITabularView.class);
			});
		});
	}

	public int getNbAccessTokens() {
		return countAccessToken.get();
	}

}
