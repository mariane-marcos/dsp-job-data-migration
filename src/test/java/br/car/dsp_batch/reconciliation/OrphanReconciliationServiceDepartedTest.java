package br.car.dsp_batch.reconciliation;

import br.car.dsp_batch.layer.config.LayerConfig;
import br.car.dsp_batch.layer.config.LayersProperties;
import br.car.dsp_batch.layer.introspection.SchemaIntrospectionService;
import br.car.dsp_batch.layer.metadata.QualifiedTable;
import br.car.dsp_batch.sync.DepartedLevel3ContextSupport;
import br.car.dsp_batch.sync.SyncKeys;
import br.car.dsp_batch.sync.WatermarkContextKeys;
import br.car.dsp_batch.sync.WatermarkTableSpec;
import br.car.dsp_batch.temporal.TemporalTestFixtures;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.springframework.batch.core.JobExecution;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OrphanReconciliationServiceDepartedTest {

    private final ReconciliationProperties properties = new ReconciliationProperties();
    private final SchemaIntrospectionService introspection = mock(SchemaIntrospectionService.class);

    @Test
    void reconcileOrphans_CapturesDepartedLevel3BeforeDelete() {
        JdbcTemplate geoTargetJdbc = mock(JdbcTemplate.class);
        JdbcTemplate businessTargetJdbc = mock(JdbcTemplate.class);
        OrphanReconciliationService service = new OrphanReconciliationService(properties);

        doAnswer(invocation -> {
            RowCallbackHandler handler = invocation.getArgument(1);
            handler.processRow(mockResultSet("l3-a"));
            return null;
        }).when(geoTargetJdbc).query(anyString(), any(RowCallbackHandler.class), any(Object[].class));
        when(geoTargetJdbc.update(anyString(), any(Object[].class))).thenReturn(1);

        JobExecution jobExecution = new JobExecution(1L);
        service.reconcileOrphans(
                geoTargetJdbc,
                businessTargetJdbc,
                areaOfInterestSpec(),
                List.of("100"),
                "0.000000,0.000000,1.000000,1.000000",
                jobExecution.getExecutionContext());

        assertEquals(Set.of("l3-a"), DepartedLevel3ContextSupport.read(jobExecution.getExecutionContext()));

        ArgumentCaptor<String> departedSql = ArgumentCaptor.forClass(String.class);
        verify(geoTargetJdbc).query(departedSql.capture(), any(RowCallbackHandler.class), any(Object[].class));
        assertEquals(
                "SELECT DISTINCT territory_level_3_id FROM dsp.area_of_interest "
                        + "WHERE id IN (?) AND territory_level_3_id IS NOT NULL",
                departedSql.getValue());
        InOrder order = inOrder(businessTargetJdbc, geoTargetJdbc);
        order.verify(businessTargetJdbc).update(
                "DELETE FROM dsp.area_of_interest WHERE id IN (?)",
                "100");
        order.verify(geoTargetJdbc).update(
                "DELETE FROM dsp.area_of_interest WHERE id IN (?)",
                "100");
    }

    @Test
    void reconcileOrphans_RemovesKpiAndLayerFeaturesBeforeAreaOfInterest() {
        JdbcTemplate geoTargetJdbc = mock(JdbcTemplate.class);
        JdbcTemplate businessTargetJdbc = mock(JdbcTemplate.class);
        LayersProperties layers = new LayersProperties();
        layers.setLayers(List.of(layer("zona-seguranca-300m"), layer("zona-seguranca-600m")));
        OrphanReconciliationService service =
                new OrphanReconciliationService(properties, layers, introspection);

        when(introspection.tableExists(eq(businessTargetJdbc), any(QualifiedTable.class))).thenReturn(true);
        when(introspection.tableExists(eq(geoTargetJdbc), eq(new QualifiedTable("dsp", "zona_seguranca_300m"))))
                .thenReturn(true);
        when(introspection.tableExists(eq(geoTargetJdbc), eq(new QualifiedTable("dsp", "zona_seguranca_600m"))))
                .thenReturn(false);
        when(businessTargetJdbc.update(anyString(), any(Object[].class))).thenReturn(1);
        when(geoTargetJdbc.update(anyString(), any(Object[].class))).thenReturn(1);

        JobExecution jobExecution = new JobExecution(3L);
        service.reconcileOrphans(
                geoTargetJdbc,
                businessTargetJdbc,
                areaOfInterestSpec(),
                List.of("73"),
                "0.000000,0.000000,1.000000,1.000000",
                jobExecution.getExecutionContext());

        InOrder order = inOrder(businessTargetJdbc, geoTargetJdbc);
        order.verify(businessTargetJdbc).update(
                "DELETE FROM \"dsp\".\"kpi_measure\" WHERE \"area_of_interest_id\" IN (?)",
                "73");
        order.verify(geoTargetJdbc).update(
                "DELETE FROM \"dsp\".\"zona_seguranca_300m\" WHERE \"area_of_interest_id\" IN (?)",
                "73");
        order.verify(businessTargetJdbc).update(
                "DELETE FROM dsp.area_of_interest WHERE id IN (?)",
                "73");
        order.verify(geoTargetJdbc).update(
                "DELETE FROM dsp.area_of_interest WHERE id IN (?)",
                "73");
        verify(geoTargetJdbc, never()).update(contains("zona_seguranca_600m"), any(Object[].class));
    }

    @Test
    void reconcileOrphans_RetriesAreaDeleteWhenGeoTargetFailsAfterBusinessDelete() {
        JdbcTemplate geoTargetJdbc = mock(JdbcTemplate.class);
        JdbcTemplate businessTargetJdbc = mock(JdbcTemplate.class);
        OrphanReconciliationService service =
                new OrphanReconciliationService(properties, new LayersProperties(), introspection);

        when(introspection.tableExists(eq(businessTargetJdbc), any(QualifiedTable.class))).thenReturn(true);
        when(businessTargetJdbc.update(anyString(), any(Object[].class))).thenReturn(1);
        when(geoTargetJdbc.update(anyString(), any(Object[].class)))
                .thenThrow(new RuntimeException("geo-target delete failed"))
                .thenReturn(1);

        JobExecution jobExecution = new JobExecution(4L);
        assertThrows(RuntimeException.class, () -> service.reconcileOrphans(
                geoTargetJdbc,
                businessTargetJdbc,
                areaOfInterestSpec(),
                List.of("73"),
                "0.000000,0.000000,1.000000,1.000000",
                jobExecution.getExecutionContext()));
        service.reconcileOrphans(
                geoTargetJdbc,
                businessTargetJdbc,
                areaOfInterestSpec(),
                List.of("73"),
                "0.000000,0.000000,1.000000,1.000000",
                jobExecution.getExecutionContext());

        verify(businessTargetJdbc, times(2)).update(
                "DELETE FROM dsp.area_of_interest WHERE id IN (?)",
                "73");
        verify(geoTargetJdbc, times(2)).update(
                "DELETE FROM dsp.area_of_interest WHERE id IN (?)",
                "73");
    }

    @Test
    void reconcileOrphans_CapturesDepartedLevel3FromLayerFeatureBeforeDelete() {
        JdbcTemplate geoTargetJdbc = mock(JdbcTemplate.class);
        String layerSyncKey = "dsp_zona_seguranca_300m";
        OrphanReconciliationService service = new OrphanReconciliationService(properties);

        doAnswer(invocation -> {
            RowCallbackHandler handler = invocation.getArgument(1);
            handler.processRow(mockResultSet("l3-layer"));
            return null;
        }).when(geoTargetJdbc).query(anyString(), any(RowCallbackHandler.class), any(Object[].class));
        when(geoTargetJdbc.update(anyString(), any(Object[].class))).thenReturn(1);

        WatermarkTableSpec spec = new WatermarkTableSpec(
                layerSyncKey,
                "src.layer",
                "id",
                "geom",
                TemporalTestFixtures.timestamptz("created_at"),
                TemporalTestFixtures.timestamptz("updated_at"),
                "1=1",
                4326,
                "zona-seguranca-300m",
                "dsp.zona_seguranca_300m",
                "id",
                "geometry",
                null,
                null,
                null,
                null
        );

        JobExecution jobExecution = new JobExecution(12L);
        service.reconcileOrphans(
                geoTargetJdbc,
                null,
                spec,
                List.of("layer-1"),
                "0.000000,0.000000,1.000000,1.000000",
                jobExecution.getExecutionContext());

        assertEquals(Set.of("l3-layer"), DepartedLevel3ContextSupport.read(jobExecution.getExecutionContext()));

        ArgumentCaptor<String> departedSql = ArgumentCaptor.forClass(String.class);
        verify(geoTargetJdbc).query(departedSql.capture(), any(RowCallbackHandler.class), any(Object[].class));
        assertEquals(
                "SELECT DISTINCT aoi.territory_level_3_id FROM dsp.zona_seguranca_300m l"
                        + " INNER JOIN dsp.area_of_interest aoi ON aoi.id = l.area_of_interest_id"
                        + " WHERE l.id IN (?) AND aoi.territory_level_3_id IS NOT NULL",
                departedSql.getValue());
    }

    @Test
    void reconcileOrphans_SkipsDepartedCaptureWhenColumnNotConfigured() {
        JdbcTemplate geoTargetJdbc = mock(JdbcTemplate.class);
        when(geoTargetJdbc.update(anyString(), any(Object[].class))).thenReturn(1);
        OrphanReconciliationService service = new OrphanReconciliationService(properties);

        WatermarkTableSpec spec = new WatermarkTableSpec(
                SyncKeys.ADMIN_UNIT_LEVEL_3,
                "src.l3",
                "id",
                "geom",
                TemporalTestFixtures.timestamptz("created_at"),
                TemporalTestFixtures.timestamptz("updated_at"),
                "1=1",
                4326,
                "territory-level-3",
                "dsp.territory_level_3",
                "id",
                "geometry",
                null,
                null,
                null,
                null
        );

        JobExecution jobExecution = new JobExecution(2L);
        service.reconcileOrphans(
                geoTargetJdbc,
                null,
                spec,
                List.of("100"),
                "0.000000,0.000000,1.000000,1.000000",
                jobExecution.getExecutionContext());

        assertEquals(Set.of(), DepartedLevel3ContextSupport.read(jobExecution.getExecutionContext()));
        verify(geoTargetJdbc, never()).query(
                anyString(), any(RowCallbackHandler.class), any(Object[].class));
    }

    @Test
    void reconcileOrphans_UnionsOrphanExtentWithDelta() {
        JdbcTemplate geoTargetJdbc = mock(JdbcTemplate.class);
        when(geoTargetJdbc.update(anyString(), any(Object[].class))).thenReturn(1);
        OrphanReconciliationService service = new OrphanReconciliationService(properties);

        JobExecution jobExecution = new JobExecution(6L);
        jobExecution.getExecutionContext().putString(
                WatermarkContextKeys.AFFECTED_EXTENT,
                "0.000000,0.000000,1.000000,1.000000");
        service.reconcileOrphans(
                geoTargetJdbc,
                null,
                areaOfInterestSpec(),
                List.of("100"),
                "2.000000,0.000000,3.000000,1.000000",
                jobExecution.getExecutionContext());

        assertEquals(
                "0.000000,0.000000,3.000000,1.000000",
                jobExecution.getExecutionContext().getString(WatermarkContextKeys.AFFECTED_EXTENT));
    }

    @Test
    void normalizeId_ConvertsNumberToString() {
        OrphanReconciliationService service = new OrphanReconciliationService(properties);
        assertEquals("42", service.normalizeId(42L));
    }

    @Test
    void reconcileOrphans_NormalizesNumericOrphanIdForDeleteBind() {
        JdbcTemplate geoTargetJdbc = mock(JdbcTemplate.class);
        JdbcTemplate businessTargetJdbc = mock(JdbcTemplate.class);
        OrphanReconciliationService service = new OrphanReconciliationService(properties);

        doAnswer(invocation -> null).when(geoTargetJdbc).query(
                anyString(), any(RowCallbackHandler.class), any(Object[].class));
        when(geoTargetJdbc.update(anyString(), any(Object[].class))).thenReturn(1);
        when(businessTargetJdbc.update(anyString(), any(Object[].class))).thenReturn(1);

        JobExecution jobExecution = new JobExecution(5L);
        service.reconcileOrphans(
                geoTargetJdbc,
                businessTargetJdbc,
                areaOfInterestSpec(),
                List.of(73),
                "0.000000,0.000000,1.000000,1.000000",
                jobExecution.getExecutionContext());

        verify(businessTargetJdbc).update(
                "DELETE FROM dsp.area_of_interest WHERE id IN (?)",
                "73");
    }

    private static LayerConfig layer(String layerName) {
        LayerConfig config = new LayerConfig();
        config.setSourceTable("public.src_layer");
        config.setLayerName(layerName);
        config.setPrimaryKey("id");
        config.setAreaOfInterestIdColumn("airport_ref_id");
        config.setCreationDateColumn("sourced_at");
        config.setGeometryColumn("the_geom");
        return config;
    }

    private static WatermarkTableSpec areaOfInterestSpec() {
        return new WatermarkTableSpec(
                SyncKeys.AREA_OF_INTEREST,
                "src.aoi",
                "id",
                "geom",
                TemporalTestFixtures.timestamptz("created_at"),
                TemporalTestFixtures.timestamptz("updated_at"),
                "1=1",
                4326,
                "area-of-interest",
                "dsp.area_of_interest",
                "id",
                "geometry",
                "dsp.area_of_interest",
                "id",
                "territory_level_3_id",
                null
        );
    }

    private static java.sql.ResultSet mockResultSet(Object value) {
        try {
            java.sql.ResultSet rs = mock(java.sql.ResultSet.class);
            when(rs.getObject(1)).thenReturn(value);
            return rs;
        } catch (java.sql.SQLException ex) {
            throw new RuntimeException(ex);
        }
    }
}
