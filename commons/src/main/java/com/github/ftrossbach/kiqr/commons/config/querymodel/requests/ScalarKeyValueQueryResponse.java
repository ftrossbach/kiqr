package com.github.ftrossbach.kiqr.commons.config.querymodel.requests;

/**
 * Created by ftr on 20/02/2017.
 */
public class ScalarKeyValueQueryResponse extends AbstractQueryResponse {

    private byte[] value;

    public ScalarKeyValueQueryResponse(){}

    public ScalarKeyValueQueryResponse(QueryStatus status, byte[] value) {
        super(status);
        this.value = value;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }
}
