package br.car.dsp_batch.layer.job;

import br.car.dsp_batch.batch.config.JobRunGuard;
import br.car.dsp_batch.layer.config.LayerConfig;
import br.car.dsp_batch.layer.config.LayersProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * Launches geographic layer migration jobs configured in YAML.
 */
@Slf4j
@Component
@Order(2)
public class LayerMigrationJobRunner implements CommandLineRunner {

    private final JobLauncher jobLauncher;
    private final LayerMigrationJobFactory jobFactory;
    private final LayersProperties properties;

    @Value("${execution-jobs.layer-jobs:false}")
    private boolean runLayerJobs;

    public LayerMigrationJobRunner(JobLauncher jobLauncher,
                                   LayerMigrationJobFactory jobFactory,
                                   LayersProperties properties) {
        this.jobLauncher = jobLauncher;
        this.jobFactory = jobFactory;
        this.properties = properties;
    }

    @Override
    public void run(String... args) throws Exception {
        if (!runLayerJobs) {
            log.info("Layer jobs are disabled (execution-jobs.layer-jobs=false)");
            return;
        }

        var layers = properties.enabledLayers();
        if (layers.isEmpty()) {
            log.info("No enabled layers in batch.layers");
            return;
        }

        log.info("Layer execution plan: {} layer(s)", layers.size());
        for (LayerConfig config : layers) {
            runLayerJob(config);
        }
    }

    private void runLayerJob(LayerConfig config) throws Exception {
        Job job = jobFactory.createJob(config);
        log.info("Starting job {} for source {}", job.getName(), config.getSourceTable());
        long startedAt = System.currentTimeMillis();

        JobExecution execution = jobLauncher.run(job,
                new org.springframework.batch.core.JobParametersBuilder()
                        .addLong("timestamp", System.currentTimeMillis())
                        .addString("layerKey", config.resolveKey())
                        .toJobParameters());

        long durationMs = System.currentTimeMillis() - startedAt;
        log.info(
                "Job {} finished with status={} in {} ms",
                job.getName(),
                execution.getStatus(),
                durationMs
        );

        if (!execution.getAllFailureExceptions().isEmpty()) {
            log.error(
                    "Job {} exitStatus={} failures={}",
                    job.getName(),
                    execution.getExitStatus(),
                    execution.getAllFailureExceptions().size()
            );
            for (Throwable failure : execution.getAllFailureExceptions()) {
                log.error("Job {} failure: {}", job.getName(), failure.getMessage(), failure);
            }
        }

        for (StepExecution step : execution.getStepExecutions()) {
            log.info(
                    "  Step {}: read={} write={} filter={} skip={} commit={}",
                    step.getStepName(),
                    step.getReadCount(),
                    step.getWriteCount(),
                    step.getFilterCount(),
                    step.getSkipCount(),
                    step.getCommitCount()
            );
        }
        JobRunGuard.requireCompleted(job.getName(), execution);
    }
}
