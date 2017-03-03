package com.github.ftrossbach.kiqr.commons.config.querymodel.requests;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;


import java.util.HashMap;
import java.util.Map;

/**
 * Created by ftr on 20/02/2017.
 */
public class MultiValuedKeyValueQueryResponse extends AbstractQueryResponse{


    private Map<String,String> results = new HashMap<>();

    public MultiValuedKeyValueQueryResponse() {
    }

    public MultiValuedKeyValueQueryResponse(QueryStatus status, Map<String,String> results) {
        super(status);
        this.results = results;
    }

    public Map<String,String> getResults() {
        return results;
    }

    public void setResults(Map<String,String> results) {
        this.results = results;
    }

    public MultiValuedKeyValueQueryResponse merge(MultiValuedKeyValueQueryResponse other){

        Map<String, String> left = new HashMap<>(this.results);
        Map<String, String> right = new HashMap<>(other.results);
        left.putAll(right);

        return new MultiValuedKeyValueQueryResponse(QueryStatus.OK, left);
    }
}
