package eu.solven.adhoc.pivotable.webmvc.security;

import org.springframework.context.annotation.Import;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;

import eu.solven.adhoc.pivotable.oauth2.resourceserver.PivotableJwtWebmvcSecurity;
import eu.solven.adhoc.pivotable.oauth2.resourceserver.PivotableResourceServerWebmvcConfiguration;
import eu.solven.adhoc.pivotable.webnone.security.PivotableSecurityWebnoneSpringConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * Everything related with security in Pivotable.
 * 
 * @author Benoit Lacelle
 */
// https://docs.spring.io/spring-security/reference/reactive/oauth2/login/advanced.html#webflux-oauth2-login-advanced-userinfo-endpoint
@EnableWebSecurity
@Import({

		// none
		PivotableSecurityWebnoneSpringConfig.class,

		// webflux
		PivotableSocialWebmvcSecurity.class,
		PivotableJwtWebmvcSecurity.class,
		PivotableResourceServerWebmvcConfiguration.class,

// PivotableLoginWebfluxController.class,
// PivotableMetadataController.class,

// JwtUserContextHolder.class,
//
// PivotableWebExceptionHandler.class,

})
@Slf4j
public class PivotableSecurityWebmvcSpringConfig {

}