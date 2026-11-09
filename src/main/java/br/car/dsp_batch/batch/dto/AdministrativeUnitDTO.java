package br.car.dsp_batch.batch.dto;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Generic DTO for administrative unit geographic records.
 * Column values (excluding geometry) are stored in {@link #attributes}.
 */
@Getter
@Setter
@NoArgsConstructor
public class AdministrativeUnitDTO {

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
