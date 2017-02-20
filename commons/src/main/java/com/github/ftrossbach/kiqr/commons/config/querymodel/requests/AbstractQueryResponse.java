package com.github.ftrossbach.kiqr.commons.config.querymodel.requests;

/**
 * Created by ftr on 20/02/2017.
 */
public class AbstractQueryResponse {

    private QueryStatus status;

    public AbstractQueryResponse(){}

    public AbstractQueryResponse(QueryStatus status) {
        this.status = status;
    }

    public void setStatus(QueryStatus status) {
        this.status = status;
    }

    public QueryStatus getStatus() {
        return status;
    }
}
