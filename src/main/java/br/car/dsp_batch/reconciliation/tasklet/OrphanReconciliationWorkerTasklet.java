package br.car.dsp_batch.reconciliation.tasklet;

import br.car.dsp_batch.reconciliation.GeoTargetOrphanBatchReader;
import br.car.dsp_batch.reconciliation.OrphanOriginProbe;
import br.car.dsp_batch.reconciliation.OrphanReconciliationService;
import br.car.dsp_batch.reconciliation.ReconciliationProperties;
import br.car.dsp_batch.reconciliation.partitioner.GeoTargetReconciliationPartitioner;
import br.car.dsp_batch.sync.WatermarkTableSpec;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Slf4j
public class OrphanReconciliationWorkerTasklet implements Tasklet {

    private final JdbcTemplate sourceJdbc;
    private final JdbcTemplate businessTargetJdbc;
    private final JdbcTemplate geoTargetJdbc;
    private final WatermarkTableSpec spec;
    private final OrphanReconciliationService reconciliationService;
    private final OrphanOriginProbe originProbe;
    private final ReconciliationProperties properties;
    private final String rangeStartExclusive;
    private final String rangeEndInclusive;

    public OrphanReconciliationWorkerTasklet(JdbcTemplate sourceJdbc,
                                             JdbcTemplate businessTargetJdbc,
                                             JdbcTemplate geoTargetJdbc,
                                             WatermarkTableSpec spec,
                                             OrphanReconciliationService reconciliationService,
                                             OrphanOriginProbe originProbe,
                                             ReconciliationProperties properties,
                                             String rangeStartExclusive,
                                             String rangeEndInclusive) {
        this.sourceJdbc = sourceJdbc;
        this.businessTargetJdbc = businessTargetJdbc;
        this.geoTargetJdbc = geoTargetJdbc;
        this.spec = spec;
        this.reconciliationService = reconciliationService;
        this.originProbe = originProbe;
        this.properties = properties;
        this.rangeStartExclusive = rangeStartExclusive;
        this.rangeEndInclusive = rangeEndInclusive;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        ExecutionContext jobContext = chunkContext.getStepContext()
                .getStepExecution()
                .getJobExecution()
                .getExecutionContext();

        GeoTargetOrphanBatchReader reader = new GeoTargetOrphanBatchReader(
                geoTargetJdbc, spec, rangeEndInclusive);

        String lastKey = rangeStartExclusive;
        int batchSize = properties.getBatchSize();
        int totalOrphans = 0;

        while (true) {
            List<String> batch = reader.fetchNextBatch(lastKey, batchSize);
            if (batch.isEmpty()) {
                break;
            }

            Set<Object> existingOnSource = originProbe.probeExistingNormalizedIds(
                    sourceJdbc, spec, batch);

            List<String> orphanIds = new ArrayList<>();
            for (String rawId : batch) {
                Object normalized = reconciliationService.normalizeId(rawId);
                if (!existingOnSource.contains(normalized)) {
                    orphanIds.add(rawId);
                }
            }

            if (!orphanIds.isEmpty()) {
                reconciliationService.reconcileOrphans(
                        geoTargetJdbc,
                        businessTargetJdbc,
                        spec,
                        orphanIds,
                        reader.fetchExtent(orphanIds),
                        jobContext);
                totalOrphans += orphanIds.size();
            }

            lastKey = batch.get(batch.size() - 1);
        }

        log.info("Reconciliation worker for syncKey={} removed {} orphan(s)", spec.syncKey(), totalOrphans);
        return RepeatStatus.FINISHED;
    }

    public static String rangeStart(ExecutionContext stepContext) {
        return stepContext.containsKey(GeoTargetReconciliationPartitioner.RANGE_START)
                ? stepContext.getString(GeoTargetReconciliationPartitioner.RANGE_START)
                : null;
    }

    public static String rangeEnd(ExecutionContext stepContext) {
        return stepContext.containsKey(GeoTargetReconciliationPartitioner.RANGE_END)
                ? stepContext.getString(GeoTargetReconciliationPartitioner.RANGE_END)
                : null;
    }
}
