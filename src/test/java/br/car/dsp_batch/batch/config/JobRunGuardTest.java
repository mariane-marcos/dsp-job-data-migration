package br.car.dsp_batch.batch.config;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobRunGuardTest {

    @Test
    void requireCompleted_ThrowsWhenJobDidNotFinish() {
        JobExecution execution = new JobExecution(1L);
        execution.setStatus(BatchStatus.FAILED);

        IllegalStateException ex = assertThrows(
                IllegalStateException.class,
                () -> JobRunGuard.requireCompleted("areaOfInterestGeoserverJob", execution));

        assertTrue(ex.getMessage().contains("FAILED"));
        assertTrue(ex.getMessage().contains("areaOfInterestGeoserverJob"));
    }

    @Test
    void requireCompleted_AcceptsCompletedJob() {
        JobExecution execution = new JobExecution(2L);
        execution.setStatus(BatchStatus.COMPLETED);

        assertDoesNotThrow(() -> JobRunGuard.requireCompleted("areaOfInterestGeoserverJob", execution));
    }
}
