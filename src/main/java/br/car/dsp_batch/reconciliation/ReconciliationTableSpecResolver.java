package br.car.dsp_batch.reconciliation;

import br.car.dsp_batch.batch.config.JobTableConfig;
import br.car.dsp_batch.batch.config.JobTableConfigValidator;
import br.car.dsp_batch.layer.metadata.QualifiedTable;
import br.car.dsp_batch.sync.WatermarkTableSpec;
import br.car.dsp_batch.sync.WatermarkTableSpecs;
import br.car.dsp_batch.temporal.BatchTemporalProperties;
import br.car.dsp_batch.temporal.TemporalColumnSpecs;
import br.car.dsp_batch.temporal.TemporalSchemaSupport;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.List;

@Component
public class ReconciliationTableSpecResolver {

    private final TemporalSchemaSupport temporalSchemaSupport;
    private final BatchTemporalProperties batchTemporalProperties;

    public ReconciliationTableSpecResolver(TemporalSchemaSupport temporalSchemaSupport,
                                           BatchTemporalProperties batchTemporalProperties) {
        this.temporalSchemaSupport = temporalSchemaSupport;
        this.batchTemporalProperties = batchTemporalProperties;
    }

    public WatermarkTableSpec fromJobTableConfig(JobTableConfig tableConfig, DataSource sourceDataSource) {
        JobTableConfigValidator.requireWatermarkFields(tableConfig);
        TemporalColumnSpecs temporalColumns = temporalSchemaSupport.resolveTemporalColumns(
                new JdbcTemplate(sourceDataSource),
                tableConfig.getSourceTable(),
                tableConfig.getCreationDateColumn(),
                tableConfig.getUpdatedAtColumn(),
                batchTemporalProperties.resolvePolicy(
                        tableConfig.getSourceTimezone(),
                        "batch.*.source-timezone for " + tableConfig.getSourceTable()));
        String sourcePkUdt = requireSourcePkUdt(
                new JdbcTemplate(sourceDataSource),
                tableConfig.getSourceTable(),
                tableConfig.getPrimaryKey());
        return WatermarkTableSpecs.fromJobTableConfig(tableConfig, temporalColumns, sourcePkUdt);
    }

    static String requireSourcePkUdt(JdbcTemplate jdbc, String qualifiedTable, String column) {
        QualifiedTable table = QualifiedTable.parse(qualifiedTable);
        List<String> udts = jdbc.query(
                """
                SELECT udt_name
                FROM information_schema.columns
                WHERE table_schema = ? AND table_name = ? AND column_name = ?
                """,
                (rs, rowNum) -> rs.getString("udt_name"),
                table.schema(),
                table.table(),
                column);
        if (udts.isEmpty() || udts.getFirst() == null || udts.getFirst().isBlank()) {
            throw new IllegalStateException(
                    "Source PK column " + column + " not found on " + qualifiedTable);
        }
        return udts.getFirst();
    }
}
