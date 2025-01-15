package eu.solven.adhoc.app;

import eu.solven.adhoc.js.webflux.AdhocWebFluxConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import({ AdhocWebFluxConfiguration.class })
public class AdhocMonolithApplication {

	public static void main(String[] args) {
		SpringApplication.run(AdhocMonolithApplication.class, args);
	}

}