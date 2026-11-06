package br.car.dsp_batch.batch.tasklet;

import br.car.dsp_batch.batch.config.JobTableConfig;
import br.car.dsp_batch.batch.config.JobTableConfigValidator;
import br.car.dsp_batch.service.AdministrativeUnitPersistenceService;
import br.car.dsp_batch.sync.WatermarkChangeDetectionEngine;
import br.car.dsp_batch.sync.WatermarkTableSpecs;
import br.car.dsp_batch.temporal.BatchTemporalProperties;
import br.car.dsp_batch.temporal.TemporalSchemaSupport;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Runs incremental watermark change detection for admin unit / AOI tables.
 */
@Slf4j
public class ChangeDetectionTasklet implements Tasklet {

    private final JdbcTemplate sourceJdbc;
    private final JdbcTemplate targetJdbc;
    private final JdbcTemplate geoTargetJdbc;
    private final JobTableConfig tableConfig;
    private final WatermarkChangeDetectionEngine engine;
    private final TemporalSchemaSupport temporalSchemaSupport;
    private final BatchTemporalProperties batchTemporalProperties;

    public ChangeDetectionTasklet(JdbcTemplate sourceJdbc,
                                  JdbcTemplate targetJdbc,
                                  JdbcTemplate geoTargetJdbc,
                                  JobTableConfig tableConfig,
                                  WatermarkChangeDetectionEngine engine,
                                  TemporalSchemaSupport temporalSchemaSupport,
                                  BatchTemporalProperties batchTemporalProperties) {
        this.sourceJdbc = sourceJdbc;
        this.targetJdbc = targetJdbc;
        this.geoTargetJdbc = geoTargetJdbc;
        this.tableConfig = tableConfig;
        this.engine = engine;
        this.temporalSchemaSupport = temporalSchemaSupport;
        this.batchTemporalProperties = batchTemporalProperties;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        log.info("Executing watermark change detection for table={}", tableConfig.getSourceTable());
        AdministrativeUnitPersistenceService.requirePositiveSrid(tableConfig);
        JobTableConfigValidator.requireWatermarkFields(tableConfig);

        var temporalColumns = temporalSchemaSupport.resolveTemporalColumns(
                sourceJdbc,
                tableConfig.getSourceTable(),
                tableConfig.getCreationDateColumn(),
                tableConfig.getUpdatedAtColumn(),
                batchTemporalProperties.resolvePolicy(
                        tableConfig.getSourceTimezone(),
                        "batch.*.source-timezone for " + tableConfig.getSourceTable()
                )
        );

        String targetCreatedAt = tableConfig.resolveTargetColumn(tableConfig.getCreationDateColumn());
        temporalSchemaSupport.requireDestinationTimestamptz(
                targetJdbc, tableConfig.getTargetTable(), targetCreatedAt);
        temporalSchemaSupport.requireDestinationTimestamptz(
                geoTargetJdbc, tableConfig.getTargetTable(), targetCreatedAt);

        if (tableConfig.getUpdatedAtColumn() != null && !tableConfig.getUpdatedAtColumn().isBlank()) {
            String targetUpdatedAt = tableConfig.resolveTargetColumn(tableConfig.getUpdatedAtColumn());
            temporalSchemaSupport.requireDestinationTimestamptz(
                    targetJdbc, tableConfig.getTargetTable(), targetUpdatedAt);
            temporalSchemaSupport.requireDestinationTimestamptz(
                    geoTargetJdbc, tableConfig.getTargetTable(), targetUpdatedAt);
        }

        engine.detectChanges(
                sourceJdbc,
                geoTargetJdbc,
                targetJdbc,
                WatermarkTableSpecs.fromJobTableConfig(tableConfig, temporalColumns, null),
                chunkContext
        );
        return RepeatStatus.FINISHED;
    }
}
