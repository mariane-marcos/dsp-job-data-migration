package br.car.dsp_batch.batch.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * Optionally launches configured administrative unit jobs on application startup.
 */
@Slf4j
@Component
@Order(1)
public class JobRunner implements CommandLineRunner {

    private final JobLauncher jobLauncher;
    private final Job adminUnitLevel1GeoserverJob;
    private final Job adminUnitLevel2GeoserverJob;
    private final Job adminUnitLevel3GeoserverJob;
    private final Job areaOfInterestGeoserverJob;

    @Value("${execution-jobs.admin-unit-level-1-geoserver-job:true}")
    private boolean runLevel1;

    @Value("${execution-jobs.admin-unit-level-2-geoserver-job:false}")
    private boolean runLevel2;

    @Value("${execution-jobs.admin-unit-level-3-geoserver-job:false}")
    private boolean runLevel3;

    @Value("${execution-jobs.area-of-interest-geoserver-job:false}")
    private boolean runAreaOfInterest;

    public JobRunner(
            JobLauncher jobLauncher,
            @Qualifier("adminUnitLevel1GeoserverJob") Job adminUnitLevel1GeoserverJob,
            @Qualifier("adminUnitLevel2GeoserverJob") Job adminUnitLevel2GeoserverJob,
            @Qualifier("adminUnitLevel3GeoserverJob") Job adminUnitLevel3GeoserverJob,
            @Qualifier("areaOfInterestGeoserverJob") Job areaOfInterestGeoserverJob) {
        this.jobLauncher = jobLauncher;
        this.adminUnitLevel1GeoserverJob = adminUnitLevel1GeoserverJob;
        this.adminUnitLevel2GeoserverJob = adminUnitLevel2GeoserverJob;
        this.adminUnitLevel3GeoserverJob = adminUnitLevel3GeoserverJob;
        this.areaOfInterestGeoserverJob = areaOfInterestGeoserverJob;
    }

    @Override
    public void run(String... args) throws Exception {
        log.info(
                "Job execution plan: {}={}, {}={}, {}={}, {}={}",
                adminUnitLevel1GeoserverJob.getName(), enabledLabel(runLevel1),
                adminUnitLevel2GeoserverJob.getName(), enabledLabel(runLevel2),
                adminUnitLevel3GeoserverJob.getName(), enabledLabel(runLevel3),
                areaOfInterestGeoserverJob.getName(), enabledLabel(runAreaOfInterest)
        );
        runJobIfEnabled(runLevel1, adminUnitLevel1GeoserverJob);
        runJobIfEnabled(runLevel2, adminUnitLevel2GeoserverJob);
        runJobIfEnabled(runLevel3, adminUnitLevel3GeoserverJob);
        runJobIfEnabled(runAreaOfInterest, areaOfInterestGeoserverJob);
    }

    private void runJobIfEnabled(boolean enabled, Job job) throws Exception {
        if (!enabled) {
            log.info("Job {} is disabled", job.getName());
            return;
        }
        log.info("Starting job {}", job.getName());
        long startedAt = System.currentTimeMillis();
        JobExecution execution = jobLauncher.run(job, new org.springframework.batch.core.JobParametersBuilder()
                .addLong("timestamp", System.currentTimeMillis())
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

    private static String enabledLabel(boolean enabled) {
        return enabled ? "enabled" : "disabled";
    }
}
