package com.github.ftrossbach.kiqr.commons.config.querymodel.requests;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.github.ftrossbach.kiqr.commons.config.rest.serde.BufferValueSerializer;
import io.vertx.core.buffer.Buffer;

import java.util.SortedMap;

/**
 * Created by ftr on 20/02/2017.
 */
public class WindowedQueryResponse extends AbstractQueryResponse{

    @JsonSerialize(contentUsing = BufferValueSerializer.class)
    private SortedMap<Long, Buffer> values;

    public WindowedQueryResponse(SortedMap<Long, Buffer> values) {
        this.values = values;
    }

    public WindowedQueryResponse(QueryStatus status, SortedMap<Long, Buffer> values) {
        super(status);
        this.values = values;
    }

    public void setValues(SortedMap<Long, Buffer> values) {
        this.values = values;
    }

    public SortedMap<Long, Buffer> getValues() {
        return values;
    }
}
