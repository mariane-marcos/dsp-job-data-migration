package br.car.dsp_batch.reconciliation.tasklet;

import br.car.dsp_batch.reconciliation.ReconciliationProperties;
import br.car.dsp_batch.sync.CycleAffectedExtent;
import br.car.dsp_batch.sync.SyncKeys;
import br.car.dsp_batch.sync.WatermarkContextKeys;
import br.car.dsp_batch.sync.WatermarkTableSpec;
import br.car.dsp_batch.temporal.TemporalTestFixtures;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ReconciliationSetupTaskletTest {

    @Test
    void execute_CopiesDeltaExtentFromThisProcess() throws Exception {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForObject(anyString(), eq(Long.class))).thenReturn(3L);
        CycleAffectedExtent holder = new CycleAffectedExtent();
        holder.replace(SyncKeys.ADMIN_UNIT_LEVEL_1, "1.000000,2.000000,3.000000,4.000000");

        JobExecution jobExecution = new JobExecution(1L);
        tasklet(jdbc, holder).execute(mock(StepContribution.class), chunkContext(jobExecution));

        assertEquals(
                "1.000000,2.000000,3.000000,4.000000",
                jobExecution.getExecutionContext().getString(WatermarkContextKeys.AFFECTED_EXTENT));
    }

    @Test
    void execute_LeavesExtentEmptyWhenThisCycleHasNoDelta() throws Exception {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForObject(anyString(), eq(Long.class))).thenReturn(0L);

        JobExecution jobExecution = new JobExecution(2L);
        tasklet(jdbc, new CycleAffectedExtent()).execute(mock(StepContribution.class), chunkContext(jobExecution));

        assertFalse(jobExecution.getExecutionContext().containsKey(WatermarkContextKeys.AFFECTED_EXTENT));
    }

    private static ReconciliationSetupTasklet tasklet(JdbcTemplate jdbc, CycleAffectedExtent holder) {
        return new ReconciliationSetupTasklet(jdbc, spec(), new ReconciliationProperties(), holder);
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
                "territory-level-1",
                "dsp.territory_level_1",
                "id",
                "geom",
                null,
                null,
                null,
                "int4"
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
