package br.car.dsp_batch.reconciliation;

import br.car.dsp_batch.sync.SyncKeys;
import br.car.dsp_batch.sync.WatermarkTableSpec;
import br.car.dsp_batch.temporal.TemporalTestFixtures;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class GeoTargetOrphanBatchReaderTest {

    @Test
    void fetchNextBatch_KeysetUsesPkColumn() {
        JdbcTemplate geoTargetJdbc = mock(JdbcTemplate.class);
        GeoTargetOrphanBatchReader reader = new GeoTargetOrphanBatchReader(
                geoTargetJdbc, sampleSpec(), "m");

        reader.fetchNextBatch("abc", 500);

        String sql = capturedSql(geoTargetJdbc);
        assertTrue(sql.contains("AND id > ?"));
        assertTrue(sql.contains("AND id <= ?"));
        assertTrue(sql.contains("ORDER BY id LIMIT ?"));
        assertFalse(sql.contains("hashtext"));
        assertFalse(sql.contains("::text"));
        assertFalse(sql.contains("ST_MakeValid"));
        assertFalse(sql.contains("ST_Envelope"));
    }

    @Test
    void fetchNextBatch_FirstPageOmitsLowerBound() {
        JdbcTemplate geoTargetJdbc = mock(JdbcTemplate.class);
        GeoTargetOrphanBatchReader reader = new GeoTargetOrphanBatchReader(
                geoTargetJdbc, sampleSpec(), null);

        reader.fetchNextBatch(null, 500);

        String sql = capturedSql(geoTargetJdbc);
        assertFalse(sql.contains("id > ?"));
        assertFalse(sql.contains("id <= ?"));
        assertTrue(sql.contains("ORDER BY id LIMIT ?"));
    }

    @Test
    void fetchBboxes_EmptyIdsSkipQuery() {
        JdbcTemplate geoTargetJdbc = mock(JdbcTemplate.class);
        GeoTargetOrphanBatchReader reader = new GeoTargetOrphanBatchReader(
                geoTargetJdbc, sampleSpec(), null);

        reader.fetchBboxes(List.of());

        verify(geoTargetJdbc, never()).execute(any(ConnectionCallback.class));
    }

    @Test
    void orphanBboxSql_LooksUpOnlyTheGivenIds() {
        String sql = GeoTargetOrphanBatchReader.orphanBboxSql(sampleSpec());
        assertTrue(sql.contains("id = ANY(?)"));
        assertTrue(sql.contains("ST_Envelope"));
        assertFalse(sql.contains("ST_MakeValid"));
        assertTrue(sql.contains("FROM dsp.territory_level_1"));
    }

    private static String capturedSql(JdbcTemplate geoTargetJdbc) {
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(geoTargetJdbc).query(sqlCaptor.capture(), any(RowCallbackHandler.class), any(Object[].class));
        return sqlCaptor.getValue();
    }

    private static WatermarkTableSpec sampleSpec() {
        return new WatermarkTableSpec(
                SyncKeys.ADMIN_UNIT_LEVEL_1,
                "src.l1",
                "id",
                "geom",
                TemporalTestFixtures.timestamptz("created_at"),
                TemporalTestFixtures.timestamptz("updated_at"),
                "1=1",
                3763,
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
}
