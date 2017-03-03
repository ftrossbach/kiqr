package com.github.ftrossbach.kiqr.commons.config.querymodel.requests;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;


import java.util.SortedMap;

/**
 * Created by ftr on 20/02/2017.
 */
public class WindowedQueryResponse extends AbstractQueryResponse{



    private SortedMap<Long, String> values;

    public WindowedQueryResponse() {

    }

    public WindowedQueryResponse(QueryStatus status, SortedMap<Long, String> values) {
        super(status);
        this.values = values;
    }

    public void setValues(SortedMap<Long, String> values) {
        this.values = values;
    }

    public SortedMap<Long, String> getValues() {
        return values;
    }
}
