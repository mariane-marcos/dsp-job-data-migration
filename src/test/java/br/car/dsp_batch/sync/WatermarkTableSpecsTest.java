package br.car.dsp_batch.sync;

import br.car.dsp_batch.batch.config.table.AdministrativeUnitTableProperties;
import br.car.dsp_batch.layer.metadata.ColumnMetadata;
import br.car.dsp_batch.layer.metadata.LayerTableMetadata;
import br.car.dsp_batch.layer.metadata.QualifiedTable;
import br.car.dsp_batch.temporal.TemporalColumnSpecs;
import br.car.dsp_batch.temporal.TemporalTestFixtures;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WatermarkTableSpecsTest {

    @Test
    void fromJobTableConfig_MapsAdminUnitFields() {
        AdministrativeUnitTableProperties config = new AdministrativeUnitTableProperties();
        config.setSourceTable("source.l1");
        config.setTargetTable("target.l1");
        config.setPrimaryKey("source_pk");
        config.setGeometryColumn("source_geom");
        config.setCreationDateColumn("source_created_at");
        config.setUpdatedAtColumn("source_updated_at");
        config.setSyncKey(SyncKeys.ADMIN_UNIT_LEVEL_1);
        config.setWhereClause("1=1");
        config.setSrid(4326);
        config.setLayerName("l1-layer");
        config.getColumnMapping().put("source_pk", "target_id");
        config.getColumnMapping().put("source_geom", "target_geom");

        TemporalColumnSpecs temporalColumns = TemporalColumnSpecs.of(
                TemporalTestFixtures.timestamptz("source_created_at"),
                TemporalTestFixtures.timestamptz("source_updated_at"));
        WatermarkTableSpec spec = WatermarkTableSpecs.fromJobTableConfig(config, temporalColumns, null);

        assertEquals(SyncKeys.ADMIN_UNIT_LEVEL_1, spec.syncKey());
        assertEquals("source_created_at", spec.sourceCreationDateColumn());
        assertEquals("source_updated_at", spec.sourceUpdatedAtColumn());
        assertEquals("target.l1", spec.geoTargetTable());
        assertEquals("target.l1", spec.businessTargetTable());
        assertEquals("target_id", spec.geoTargetPrimaryKey());
        assertEquals("target_geom", spec.geoTargetGeometryColumn());
        assertNull(spec.sourcePrimaryKeyUdtName());
    }

    @Test
    void fromJobTableConfig_RequiresCreationDateColumn() {
        AdministrativeUnitTableProperties config = new AdministrativeUnitTableProperties();
        config.setSourceTable("source.l1");
        config.setSyncKey(SyncKeys.ADMIN_UNIT_LEVEL_1);

        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> WatermarkTableSpecs.fromJobTableConfig(config, null, null)
        );
        assertTrue(ex.getMessage().contains("creation-date-column"));
    }

    @Test
    void fromLayerMetadata_OmitsBusinessTarget() {
        LayerTableMetadata metadata = new LayerTableMetadata(
                "dsp_parcelas",
                "parcelas",
                new QualifiedTable("src", "parcelas"),
                new QualifiedTable("dsp", "parcelas"),
                "id",
                "geom",
                "aoi_fk",
                TemporalTestFixtures.timestamptz("created_at"),
                TemporalTestFixtures.timestamptz("updated_at"),
                "nome",
                4326,
                List.of(new ColumnMetadata("id", "int8", null, null, null, false, false)),
                List.of(),
                "1=1"
        );

        WatermarkTableSpec spec = WatermarkTableSpecs.fromLayerMetadata(metadata);

        assertEquals("dsp_parcelas", spec.syncKey());
        assertEquals("dsp.parcelas", spec.geoTargetTable());
        assertNull(spec.businessTargetTable());
        assertEquals("int8", spec.sourcePrimaryKeyUdtName());
    }

    @Test
    void fromLayerMetadata_RequiresPkType() {
        LayerTableMetadata metadata = new LayerTableMetadata(
                "dsp_parcelas",
                "parcelas",
                new QualifiedTable("src", "parcelas"),
                new QualifiedTable("dsp", "parcelas"),
                "id",
                "geom",
                "aoi_fk",
                TemporalTestFixtures.timestamptz("created_at"),
                TemporalTestFixtures.timestamptz("updated_at"),
                "nome",
                4326,
                List.of(),
                List.of(),
                "1=1"
        );

        IllegalStateException ex = assertThrows(
                IllegalStateException.class,
                () -> WatermarkTableSpecs.fromLayerMetadata(metadata));
        assertTrue(ex.getMessage().contains("id"));
    }
}
