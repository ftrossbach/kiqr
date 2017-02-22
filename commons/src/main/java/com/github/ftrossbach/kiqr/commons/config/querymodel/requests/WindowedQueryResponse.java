package com.github.ftrossbach.kiqr.commons.config.querymodel.requests;

import java.util.SortedMap;

/**
 * Created by ftr on 20/02/2017.
 */
public class WindowedQueryResponse extends AbstractQueryResponse{

    private SortedMap<Long, byte[]> values;

    public WindowedQueryResponse(SortedMap<Long, byte[]> values) {
        this.values = values;
    }

    public WindowedQueryResponse(QueryStatus status, SortedMap<Long, byte[]> values) {
        super(status);
        this.values = values;
    }

    public void setValues(SortedMap<Long, byte[]> values) {
        this.values = values;
    }

    public SortedMap<Long, byte[]> getResults() {
        return values;
    }
}
