package br.car.dsp_batch.sync;

import br.car.dsp_batch.temporal.WatermarkColumnSpec;

/**
 * Neutral table descriptor for incremental change detection.
 * Used by admin units/AOI and by layers.
 */
public record WatermarkTableSpec(
        String syncKey,
        String sourceTable,
        String sourcePrimaryKey,
        String sourceGeometryColumn,
        WatermarkColumnSpec creationDateColumn,
        WatermarkColumnSpec updatedAtColumn,
        String whereClause,
        int srid,
        String layerName,
        String geoTargetTable,
        String geoTargetPrimaryKey,
        String geoTargetGeometryColumn,
        /** When null, orphans are removed only from the geo-target. */
        String businessTargetTable,
        String businessTargetPrimaryKey,
        /**
         * When set, orphan deletion records distinct values of this column (e.g.
         * {@code territory_level_3_id}) into {@link WatermarkContextKeys#DEPARTED_LEVEL_3_IDS}.
         */
        String geoTargetDepartedTerritoryColumn,
        /**
         * PostgreSQL {@code udt_name} of the source primary key ({@code int4}, {@code int8},
         * {@code varchar}, …). Null on change-detection specs that never probe orphans.
         */
        String sourcePrimaryKeyUdtName
) {
    public String sourceCreationDateColumn() {
        return creationDateColumn.sourceColumn();
    }

    public String sourceUpdatedAtColumn() {
        return updatedAtColumn == null ? null : updatedAtColumn.sourceColumn();
    }

    public boolean hasUpdatedAtColumn() {
        return updatedAtColumn != null;
    }
}
