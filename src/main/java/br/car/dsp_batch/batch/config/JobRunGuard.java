package br.car.dsp_batch.batch.config;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;

/**
 * Stops the migration cycle when a job does not finish successfully.
 */
public final class JobRunGuard {

    private JobRunGuard() {
    }

    public static void requireCompleted(String jobName, JobExecution execution) {
        if (execution.getStatus() != BatchStatus.COMPLETED) {
            throw new IllegalStateException(
                    "Job " + jobName + " finished with status " + execution.getStatus());
        }
    }
}
