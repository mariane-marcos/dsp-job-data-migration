package br.car.dsp_batch.reconciliation.partitioner;

import br.car.dsp_batch.sync.SyncKeys;
import br.car.dsp_batch.sync.WatermarkTableSpec;
import br.car.dsp_batch.temporal.TemporalTestFixtures;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class GeoTargetReconciliationPartitionerTest {

    @Test
    void partition_SingleWorkerSkipsBoundaryQueries() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        GeoTargetReconciliationPartitioner partitioner = new GeoTargetReconciliationPartitioner(jdbc, spec());

        Map<String, ExecutionContext> partitions = partitioner.partition(1);

        assertEquals(1, partitions.size());
        assertFalse(partitions.get("partition0").containsKey(GeoTargetReconciliationPartitioner.RANGE_START));
        assertFalse(partitions.get("partition0").containsKey(GeoTargetReconciliationPartitioner.RANGE_END));
        verify(jdbc, never()).queryForObject(anyString(), eq(Long.class));
    }

    @Test
    void partition_RangesMeetWithoutOverlap() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForObject(contains("COUNT"), eq(Long.class))).thenReturn(4L);
        when(jdbc.query(anyString(), any(RowMapper.class), any())).thenAnswer(invocation -> {
            long offset = ((Number) invocation.getArgument(2)).longValue();
            return List.of("k" + offset);
        });
        GeoTargetReconciliationPartitioner partitioner = new GeoTargetReconciliationPartitioner(jdbc, spec());

        Map<String, ExecutionContext> partitions = partitioner.partition(4);

        assertEquals("k1", partitions.get("partition0").getString(GeoTargetReconciliationPartitioner.RANGE_END));
        assertFalse(partitions.get("partition0").containsKey(GeoTargetReconciliationPartitioner.RANGE_START));
        assertEquals("k1", partitions.get("partition1").getString(GeoTargetReconciliationPartitioner.RANGE_START));
        assertEquals("k2", partitions.get("partition1").getString(GeoTargetReconciliationPartitioner.RANGE_END));
        assertEquals("k2", partitions.get("partition2").getString(GeoTargetReconciliationPartitioner.RANGE_START));
        assertEquals("k3", partitions.get("partition2").getString(GeoTargetReconciliationPartitioner.RANGE_END));
        assertEquals("k3", partitions.get("partition3").getString(GeoTargetReconciliationPartitioner.RANGE_START));
        assertFalse(partitions.get("partition3").containsKey(GeoTargetReconciliationPartitioner.RANGE_END));
        verify(jdbc).query(contains("ORDER BY id"), any(RowMapper.class), eq(1L));
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
                null,
                null,
                null,
                "int4"
        );
    }
}
