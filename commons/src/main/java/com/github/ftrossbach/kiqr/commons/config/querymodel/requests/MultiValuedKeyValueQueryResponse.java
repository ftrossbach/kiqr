package com.github.ftrossbach.kiqr.commons.config.querymodel.requests;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.github.ftrossbach.kiqr.commons.config.rest.serde.BufferKeySerializer;
import com.github.ftrossbach.kiqr.commons.config.rest.serde.BufferValueSerializer;
import io.vertx.core.buffer.Buffer;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ftr on 20/02/2017.
 */
public class MultiValuedKeyValueQueryResponse extends AbstractQueryResponse{

    @JsonSerialize(keyUsing = BufferKeySerializer.class, contentUsing = BufferValueSerializer.class)
    private Map<Buffer,Buffer> results = new HashMap<>();

    public MultiValuedKeyValueQueryResponse() {
    }

    public MultiValuedKeyValueQueryResponse(QueryStatus status, Map<Buffer,Buffer> results) {
        super(status);
        this.results = results;
    }

    public Map<Buffer,Buffer> getResults() {
        return results;
    }

    public void setResults(Map<Buffer,Buffer> results) {
        this.results = results;
    }

    public MultiValuedKeyValueQueryResponse merge(MultiValuedKeyValueQueryResponse other){

        Map<Buffer, Buffer> left = new HashMap<>(this.results);
        Map<Buffer, Buffer> right = new HashMap<>(other.results);
        left.putAll(right);

        return new MultiValuedKeyValueQueryResponse(QueryStatus.OK, left);
    }
}
