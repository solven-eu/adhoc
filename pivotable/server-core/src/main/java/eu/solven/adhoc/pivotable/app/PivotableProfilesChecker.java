package eu.solven.adhoc.pivotable.app;

import com.google.common.base.Strings;
import eu.solven.adhoc.app.IPivotableSpringProfiles;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.EnvironmentPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Profiles;

/**
 * Checks the spring profiles relative to Pivotable. Typically useful to detect the lack of `spring.config.import: classpath:pivotable-config.yml`
 */
// Loaded by ./src/main/resources/META-INF/spring.factories
@Slf4j
public class PivotableProfilesChecker implements EnvironmentPostProcessor {

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment env, SpringApplication app) {
        String configImport = env.getProperty(IPivotableSpringProfiles.K_CONFIG_IMPORT);

        if (Strings.isNullOrEmpty(configImport)) {
            log.warn("Your `application.yml` (or `application.properties`) is missing {}", IPivotableSpringProfiles.P_CONFIG_IMPORT);
        } else if (!configImport.contains(IPivotableSpringProfiles.C_CONFIG)) {
            log.warn("Your `application.yml` (or `application.properties`)  {} given {}={}", IPivotableSpringProfiles.C_CONFIG, IPivotableSpringProfiles.K_CONFIG_IMPORT, configImport);
        } else if (!env.acceptsProfiles(Profiles.of(IPivotableSpringProfiles.P_PIVOTABLE))) {
            log.warn("Conflicting configuration around {}={}", IPivotableSpringProfiles.P_CONFIG_IMPORT, configImport);
        }
    }
}
