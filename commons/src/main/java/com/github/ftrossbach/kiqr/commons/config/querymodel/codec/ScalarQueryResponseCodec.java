package com.github.ftrossbach.kiqr.commons.config.querymodel.codec;

import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.ScalarKeyValueQueryResponse;

/**
 * Created by ftr on 20/02/2017.
 */
public class ScalarQueryResponseCodec extends AbstractCodec<ScalarKeyValueQueryResponse> {
    @Override
    protected int getObjectId() {
        return 3;
    }

    @Override
    protected Class<ScalarKeyValueQueryResponse> getObjectClass() {
        return ScalarKeyValueQueryResponse.class;
    }
}
