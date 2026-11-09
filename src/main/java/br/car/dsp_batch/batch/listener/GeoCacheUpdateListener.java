package br.car.dsp_batch.batch.listener;

import br.car.dsp_batch.sync.WatermarkContextKeys;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;

/**
 * Hands the run's single EPSG:3857 extent to cache invalidation.
 *
 * TODO: truncate the GeoWebCache tile layer for {@code layerName} + this extent
 * (POST seed with operation truncate). Today the listener only logs the request.
 */
@Slf4j
@Component
public class GeoCacheUpdateListener implements JobExecutionListener {

    @Override
    public void beforeJob(JobExecution jobExecution) {
        log.info("Job {} started — preparing cache update collection", jobExecution.getJobInstance().getJobName());
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        String extent = jobExecution.getExecutionContext()
                .getString(WatermarkContextKeys.AFFECTED_EXTENT, null);
        if (extent == null || extent.isBlank()) {
            log.info("No cache update required for job {}", jobExecution.getJobInstance().getJobName());
            return;
        }

        String layerName = jobExecution.getExecutionContext()
                .getString(WatermarkContextKeys.LAYER_NAME, null);
        if (layerName == null || layerName.isEmpty()) {
            log.error("Layer name was not set in the job execution context.");
            return;
        }

        log.info("Cache update requested for layer '{}' extent={} (EPSG:3857)", layerName, extent);
        // TODO: Call GeoServer Exhibition (GWC seed truncate) for layerName + extent.
    }
}
