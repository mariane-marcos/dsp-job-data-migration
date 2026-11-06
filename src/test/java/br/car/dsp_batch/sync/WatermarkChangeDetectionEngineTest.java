package br.car.dsp_batch.sync;

import br.car.dsp_batch.temporal.TemporalTestFixtures;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;

import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class WatermarkChangeDetectionEngineTest {

    private final SyncStateRepository syncStateRepository = mock(SyncStateRepository.class);
    private final WatermarkChangeDetectionEngine engine =
            new WatermarkChangeDetectionEngine(syncStateRepository);

    @Test
    void detectChanges_DoesNotRunOrphanScanOnSource() {
        JdbcTemplate sourceJdbc = mock(JdbcTemplate.class);
        JdbcTemplate geoTargetJdbc = mock(JdbcTemplate.class);
        JdbcTemplate businessTargetJdbc = mock(JdbcTemplate.class);

        when(syncStateRepository.findBySyncKey(SyncKeys.ADMIN_UNIT_LEVEL_1)).thenReturn(Optional.of(
                new SyncState(
                        SyncKeys.ADMIN_UNIT_LEVEL_1,
                        "src.l1",
                        Instant.parse("2026-08-01T00:00:00Z"),
                        Instant.parse("2026-08-01T00:00:00Z"),
                        1L,
                        Instant.now()
                )));

        WatermarkTableSpec spec = new WatermarkTableSpec(
                SyncKeys.ADMIN_UNIT_LEVEL_1,
                "src.l1",
                "id",
                "geom",
                TemporalTestFixtures.timestamptz("created_at"),
                TemporalTestFixtures.timestamptz("updated_at"),
                "1=1",
                4326,
                "layer",
                "dsp.l1",
                "id",
                "geom",
                "dsp.l1",
                "id",
                null,
                null
        );

        JobExecution jobExecution = new JobExecution(1L);
        engine.detectChanges(sourceJdbc, geoTargetJdbc, businessTargetJdbc, spec, chunkContext(jobExecution));

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(sourceJdbc).query(sqlCaptor.capture(), any(RowCallbackHandler.class));
        assertFalse(sqlCaptor.getValue().contains("Orphan check"));
        verify(geoTargetJdbc, never()).query(anyString(), any(RowCallbackHandler.class));
        verify(geoTargetJdbc, never()).update(contains("DELETE"), any(Object[].class));
    }

    private static ChunkContext chunkContext(JobExecution jobExecution) {
        StepContext stepContext = mock(StepContext.class);
        org.springframework.batch.core.StepExecution stepExecution =
                mock(org.springframework.batch.core.StepExecution.class);
        when(stepContext.getStepExecution()).thenReturn(stepExecution);
        when(stepExecution.getJobExecution()).thenReturn(jobExecution);
        return new ChunkContext(stepContext);
    }
}
