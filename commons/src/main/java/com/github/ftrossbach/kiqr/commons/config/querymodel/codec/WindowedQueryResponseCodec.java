package com.github.ftrossbach.kiqr.commons.config.querymodel.codec;

import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.WindowedQueryResponse;

/**
 * Created by ftr on 20/02/2017.
 */
public class WindowedQueryResponseCodec extends AbstractCodec<WindowedQueryResponse> {
    @Override
    protected int getObjectId() {
        return 5;
    }

    @Override
    protected Class<WindowedQueryResponse> getObjectClass() {
        return WindowedQueryResponse.class;
    }
}
