package br.car.dsp_batch.sync;

/**
 * Job execution context keys used by watermark-based change detection and commit.
 */
public final class WatermarkContextKeys {

    public static final String HAS_CHANGES = "hasChanges";
    /**
     * One box in EPSG:3857 ({@code minx,miny,maxx,maxy}) covering every changed
     * or removed geometry of the run. Replaces the old per-record bbox list.
     */
    public static final String AFFECTED_EXTENT = "affectedExtent";
    public static final String LAYER_NAME = "layerName";
    public static final String PROPOSED_WATERMARK = "proposedWatermark";
    /**
     * Watermark in force when the run started. Read after the job to know which window
     * was processed — {@link #PROPOSED_WATERMARK} already describes the new position.
     */
    public static final String PREVIOUS_WATERMARK = "previousWatermark";
    public static final String ORPHAN_CHECK_RAN = "orphanCheckRan";
    public static final String SYNC_KEY = "syncKey";
    public static final String SOURCE_TABLE = "sourceTable";
    /**
     * Level 3 territories that lost AOIs during the run (previous {@code territory_level_3_id}
     * before upsert or orphan delete). Used to flag stale territorial download files.
     */
    public static final String DEPARTED_LEVEL_3_IDS = "departedLevel3Ids";

    private WatermarkContextKeys() {
    }
}
