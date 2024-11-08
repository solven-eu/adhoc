package eu.solven.adhoc.eventbus;

import org.greenrobot.eventbus.Subscribe;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AdhocEventsToSfl4j {
	@Subscribe
	public void onMeasuratorIsCompleted(MeasuratorIsCompleted event) {
		log.info("measurator={} is completed with size={}", event.getMeasurator(), event.getNbCells());
	}
}
