package br.car.dsp_batch.sync;

import br.car.dsp_batch.temporal.TemporalTestFixtures;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

import java.sql.ResultSet;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class WatermarkChangeDetectionEngineTest {

    private final SyncStateRepository syncStateRepository = mock(SyncStateRepository.class);
    private final CycleAffectedExtent cycleAffectedExtent = new CycleAffectedExtent();
    private final WatermarkChangeDetectionEngine engine =
            new WatermarkChangeDetectionEngine(syncStateRepository, cycleAffectedExtent);

    @Test
    void detectChanges_AggregatesExtentAndDoesNotScanOrphans() throws Exception {
        JdbcTemplate sourceJdbc = mock(JdbcTemplate.class);
        JdbcTemplate geoTargetJdbc = mock(JdbcTemplate.class);
        JdbcTemplate businessTargetJdbc = mock(JdbcTemplate.class);
        stubDelta(sourceJdbc, 2L, "2026-08-02T00:00:00Z", "2026-08-03T03:00:00Z", false);

        when(syncStateRepository.findBySyncKey(SyncKeys.ADMIN_UNIT_LEVEL_1)).thenReturn(Optional.of(
                new SyncState(
                        SyncKeys.ADMIN_UNIT_LEVEL_1,
                        "src.l1",
                        Instant.parse("2026-08-01T00:00:00Z"),
                        Instant.parse("2026-08-01T00:00:00Z"),
                        1L,
                        Instant.now()
                )));

        JobExecution jobExecution = new JobExecution(1L);
        engine.detectChanges(sourceJdbc, geoTargetJdbc, businessTargetJdbc, spec(), chunkContext(jobExecution));

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(sourceJdbc).query(sqlCaptor.capture(), any(ResultSetExtractor.class));
        String sql = sqlCaptor.getValue();
        assertTrue(sql.contains("COUNT(*)"));
        assertTrue(sql.contains("ST_Extent"));
        assertTrue(sql.contains("3857"));
        assertFalse(sql.contains("env3857"));
        assertFalse(sql.contains("Orphan check"));
        verify(geoTargetJdbc, never()).query(anyString(), any(org.springframework.jdbc.core.RowCallbackHandler.class));
        verify(geoTargetJdbc, never()).update(contains("DELETE"), any(Object[].class));

        assertEquals(Boolean.TRUE, jobExecution.getExecutionContext().get(WatermarkContextKeys.HAS_CHANGES));
        assertEquals(
                "1.000000,2.000000,3.000000,4.000000",
                jobExecution.getExecutionContext().getString(WatermarkContextKeys.AFFECTED_EXTENT));
        assertEquals(
                "2026-08-03T03:00:00Z",
                jobExecution.getExecutionContext().getString(WatermarkContextKeys.PROPOSED_WATERMARK));
        assertFalse(jobExecution.getExecutionContext().containsKey("affectedBboxes"));
        assertEquals(
                "1.000000,2.000000,3.000000,4.000000",
                cycleAffectedExtent.get(SyncKeys.ADMIN_UNIT_LEVEL_1));
    }

    @Test
    void detectChanges_EmptyDeltaClearsExtent() throws Exception {
        JdbcTemplate sourceJdbc = mock(JdbcTemplate.class);
        cycleAffectedExtent.replace(SyncKeys.ADMIN_UNIT_LEVEL_1, "9.000000,9.000000,9.000000,9.000000");
        stubDelta(sourceJdbc, 0L, null, null, true);
        when(syncStateRepository.findBySyncKey(SyncKeys.ADMIN_UNIT_LEVEL_1)).thenReturn(Optional.empty());

        JobExecution jobExecution = new JobExecution(2L);
        engine.detectChanges(
                sourceJdbc, mock(JdbcTemplate.class), mock(JdbcTemplate.class), spec(), chunkContext(jobExecution));

        assertEquals(Boolean.FALSE, jobExecution.getExecutionContext().get(WatermarkContextKeys.HAS_CHANGES));
        assertFalse(jobExecution.getExecutionContext().containsKey(WatermarkContextKeys.AFFECTED_EXTENT));
        assertNull(cycleAffectedExtent.get(SyncKeys.ADMIN_UNIT_LEVEL_1));
    }

    private static void stubDelta(JdbcTemplate sourceJdbc,
                                  long count,
                                  String created,
                                  String updated,
                                  boolean extentNull) throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true);
        when(rs.getLong("delta_count")).thenReturn(count);
        when(rs.getObject(eq("max_created_at"), eq(OffsetDateTime.class)))
                .thenReturn(created == null ? null : OffsetDateTime.parse(created));
        when(rs.getObject(eq("max_updated_at"), eq(OffsetDateTime.class)))
                .thenReturn(updated == null ? null : OffsetDateTime.parse(updated));
        when(rs.getDouble(anyString())).thenReturn(1d, 2d, 3d, 4d);
        when(rs.wasNull()).thenReturn(extentNull);
        when(sourceJdbc.query(anyString(), any(ResultSetExtractor.class))).thenAnswer(invocation -> {
            ResultSetExtractor<?> extractor = invocation.getArgument(1);
            return extractor.extractData(rs);
        });
    }

    private static WatermarkTableSpec spec() {
        return new WatermarkTableSpec(
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
