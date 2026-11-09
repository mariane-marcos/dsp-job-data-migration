package br.car.dsp_batch.reconciliation.tasklet;

import br.car.dsp_batch.reconciliation.ReconciliationProperties;
import br.car.dsp_batch.sync.CycleAffectedExtent;
import br.car.dsp_batch.sync.ReconciliationContextKeys;
import br.car.dsp_batch.sync.WatermarkContextKeys;
import br.car.dsp_batch.sync.WatermarkTableSpec;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Light validation and geo-target empty check before orphan reconciliation.
 */
@Slf4j
public class ReconciliationSetupTasklet implements Tasklet {

    private final JdbcTemplate geoTargetJdbc;
    private final WatermarkTableSpec spec;
    private final ReconciliationProperties properties;
    private final CycleAffectedExtent cycleAffectedExtent;

    public ReconciliationSetupTasklet(JdbcTemplate geoTargetJdbc,
                                      WatermarkTableSpec spec,
                                      ReconciliationProperties properties,
                                      CycleAffectedExtent cycleAffectedExtent) {
        this.geoTargetJdbc = geoTargetJdbc;
        this.spec = spec;
        this.properties = properties;
        this.cycleAffectedExtent = cycleAffectedExtent;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        if (spec.syncKey() == null || spec.syncKey().isBlank()) {
            throw new IllegalArgumentException("Reconciliation requires sync-key for " + spec.sourceTable());
        }

        var jobContext = chunkContext.getStepContext()
                .getStepExecution()
                .getJobExecution()
                .getExecutionContext();

        jobContext.putString(WatermarkContextKeys.SYNC_KEY, spec.syncKey());
        jobContext.putString(WatermarkContextKeys.SOURCE_TABLE, spec.sourceTable());
        jobContext.put(ReconciliationContextKeys.ORPHAN_RECONCILIATION, true);
        if (spec.layerName() != null) {
            jobContext.putString(WatermarkContextKeys.LAYER_NAME, spec.layerName());
        }
        // Reconciliation is a later job; the delta extent lives on this process.
        String deltaExtent = cycleAffectedExtent.get(spec.syncKey());
        if (deltaExtent != null && !deltaExtent.isBlank()) {
            jobContext.putString(WatermarkContextKeys.AFFECTED_EXTENT, deltaExtent);
        }

        long rowCount = countGeoTargetRows();
        log.info("Reconciliation setup for syncKey={} geo-target rows={}",
                spec.syncKey(), rowCount);

        boolean targetEmpty = rowCount == 0;
        jobContext.put(ReconciliationContextKeys.TARGET_EMPTY, targetEmpty);
        if (targetEmpty && properties.isSkipWhenTargetEmpty()) {
            log.info("Geo-target empty — skipping orphan reconciliation for {}", spec.syncKey());
        }
        return RepeatStatus.FINISHED;
    }

    private long countGeoTargetRows() {
        Long count = geoTargetJdbc.queryForObject(
                "SELECT COUNT(*) FROM " + spec.geoTargetTable(), Long.class);
        return count == null ? 0L : count;
    }
}
