package com.github.ftrossbach.kiqr.commons.config.querymodel.requests;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ftr on 20/02/2017.
 */
public class MultiValuedKeyValueQueryResponse extends AbstractQueryResponse{

    private Map<byte[],byte[]> results = new HashMap<>();

    public MultiValuedKeyValueQueryResponse() {
    }

    public MultiValuedKeyValueQueryResponse(QueryStatus status, Map<byte[],byte[]> results) {
        super(status);
        this.results = results;
    }

    public Map<byte[],byte[]> getResults() {
        return results;
    }

    public void setResults(Map<byte[],byte[]> results) {
        this.results = results;
    }

    public MultiValuedKeyValueQueryResponse merge(MultiValuedKeyValueQueryResponse other){

        Map<byte[], byte[]> left = new HashMap<>(this.results);
        Map<byte[], byte[]> right = new HashMap<>(other.results);
        left.putAll(right);

        return new MultiValuedKeyValueQueryResponse(QueryStatus.OK, left);
    }
}
