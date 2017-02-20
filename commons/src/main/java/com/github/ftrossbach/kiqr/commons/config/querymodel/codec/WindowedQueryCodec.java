package com.github.ftrossbach.kiqr.commons.config.querymodel.codec;

import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.WindowedQuery;

/**
 * Created by ftr on 20/02/2017.
 */
public class WindowedQueryCodec extends AbstractCodec<WindowedQuery> {
    @Override
    protected int getObjectId() {
        return 2;
    }

    @Override
    protected Class<WindowedQuery> getObjectClass() {
        return WindowedQuery.class;
    }
}
