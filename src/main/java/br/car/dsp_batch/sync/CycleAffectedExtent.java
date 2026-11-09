package br.car.dsp_batch.sync;

import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Delta extent of the current process, keyed by sync key.
 * Reconciliation is a later job and reads this to union orphan removals.
 */
@Component
public class CycleAffectedExtent {

    private final ConcurrentHashMap<String, String> extents = new ConcurrentHashMap<>();

    public void replace(String syncKey, String extent) {
        if (syncKey == null || syncKey.isBlank()) {
            return;
        }
        if (extent == null || extent.isBlank()) {
            extents.remove(syncKey);
            return;
        }
        extents.put(syncKey, extent);
    }

    public String get(String syncKey) {
        return syncKey == null ? null : extents.get(syncKey);
    }
}
