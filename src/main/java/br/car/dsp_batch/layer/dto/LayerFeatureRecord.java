package br.car.dsp_batch.layer.dto;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One feature — a single row read from a layer source table.
 */
@Getter
@Setter
@NoArgsConstructor
public class LayerFeatureRecord {

    private Object id;
    private byte[] geometryEwkb;
    private Map<String, Object> attributes = new LinkedHashMap<>();

    public Object getAttribute(String column) {
        return attributes.get(column);
    }

    public void putAttribute(String column, Object value) {
        attributes.put(column, value);
    }
}
