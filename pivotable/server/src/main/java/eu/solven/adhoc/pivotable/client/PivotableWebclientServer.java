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
import org.springframework.web.reactive.function.client.WebClient.RequestBodySpec;
import org.springframework.web.reactive.function.client.WebClient.RequestHeadersSpec;

import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import eu.solven.adhoc.beta.schema.QueryOnSchema;
import eu.solven.adhoc.beta.schema.SchemaMetadata;
import eu.solven.adhoc.pivotable.entrypoint.AdhocEntrypointMetadata;
import eu.solven.adhoc.pivotable.entrypoint.AdhocEntrypointSearch;
import eu.solven.adhoc.pivotable.login.AccessTokenWrapper;
import eu.solven.adhoc.storage.ITabularView;
import eu.solven.adhoc.storage.ListBasedTabularView;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * See routes as defined in KumiteRouter
 * 
 * @author Benoit Lacelle
 *
 */
// https://www.baeldung.com/spring-5-webclient
@Slf4j
public class PivotableWebclientServer implements IPivotableServer {
	final String PREFIX = "/api/v1";

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
	Mono<AccessTokenWrapper> accessToken() {
		// Return the same Mono for 45 minutes
		return requestAccessToken()
				// Given Mono will see its value cache for 45 minutes
				// BEWARE This will not automatically discard the cached value if we get (for any reason) a 401
				.cache(Duration.ofMinutes(45));
	}

	// see PivotableEntrypointsHandler
	@Override
	public Flux<AdhocEntrypointMetadata> searchEntrypoints(AdhocEntrypointSearch search) {
		return accessToken().map(accessToken -> {
			RequestHeadersSpec<?> spec = getWebClient().get()
					.uri(uriBuilder -> uriBuilder.path(PREFIX + "/entrypoints")
							.queryParamIfPresent("entrypoint_id", search.getEntrypointId())
							.queryParamIfPresent("keyword", search.getKeyword())
							.build())
					.header(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken.getAccessToken());

			return spec;
		}).flatMapMany(spec -> {
			return spec.exchangeToFlux(r -> {
				if (!r.statusCode().is2xxSuccessful()) {
					throw new IllegalArgumentException("Request rejected: " + r.statusCode());
				}
				log.info("Search for entrypoints: {}", r.statusCode());
				return r.bodyToFlux(AdhocEntrypointMetadata.class);
			});
		});
	}

	@Override
	public Flux<SchemaMetadata> searchSchemas(AdhocEntrypointSearch search) {
		return accessToken().map(accessToken -> {
			RequestHeadersSpec<?> spec = getWebClient().get()
					.uri(uriBuilder -> uriBuilder.path(PREFIX + "/schemas")
							.queryParamIfPresent("entrypoint_id", search.getEntrypointId())
							.queryParamIfPresent("keyword", search.getKeyword())
							.build())
					.header(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken.getAccessToken());

			return spec;
		}).flatMapMany(spec -> {
			return spec.exchangeToFlux(r -> {
				if (!r.statusCode().is2xxSuccessful()) {
					throw new IllegalArgumentException("Request rejected: " + r.statusCode());
				}
				log.info("Search for schemas: {}", r.statusCode());
				return r.bodyToFlux(SchemaMetadata.class);
			});
		});
	}

	@Override
	public Mono<ITabularView> executeQuery(QueryOnSchema query) {
		return accessToken().map(accessToken -> {
			RequestBodySpec spec = getWebClient().post().uri(uriBuilder -> {
				return uriBuilder.path(PREFIX + "/cubes/query").build();
			}).header(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken.getAccessToken());

			return spec;
		}).flatMap(spec -> {
			return spec.bodyValue(query).exchangeToMono(r -> {
				if (!r.statusCode().is2xxSuccessful()) {
					throw new IllegalArgumentException("Request rejected: " + r.statusCode());
				}
				log.info("Execute a query: {}", r.statusCode());
				return r.bodyToMono(ListBasedTabularView.class);
			});
		});
	}

	public int getNbAccessTokens() {
		return countAccessToken.get();
	}

}
