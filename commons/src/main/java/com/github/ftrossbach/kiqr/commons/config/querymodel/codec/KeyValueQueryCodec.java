package com.github.ftrossbach.kiqr.commons.config.querymodel.codec;

import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.ScalarKeyValueQuery;

/**
 * Created by ftr on 20/02/2017.
 */
public class KeyValueQueryCodec extends AbstractCodec<ScalarKeyValueQuery> {

    @Override
    protected int getObjectId() {
        return 1;
    }

    @Override
    protected Class<ScalarKeyValueQuery> getObjectClass() {
        return ScalarKeyValueQuery.class;
    }
}
