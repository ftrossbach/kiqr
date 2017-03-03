package com.github.ftrossbach.kiqr.commons.config.querymodel.requests;

import java.util.Arrays;

/**
 * Created by ftr on 20/02/2017.
 */
public class ScalarKeyValueQueryResponse extends AbstractQueryResponse {

    private String value;

    public ScalarKeyValueQueryResponse(){}

    public ScalarKeyValueQueryResponse(QueryStatus status, String value) {
        super(status);
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }


    @Override
    public String toString() {
        return "ScalarKeyValueQueryResponse{" +
                "value=" + value +
                '}';
    }
}
