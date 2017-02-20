package com.github.ftrossbach.kiqr.commons.config.querymodel.codec;

import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.MultiValuedQueryResponse;

/**
 * Created by ftr on 20/02/2017.
 */
public class MultiValuedQueryResponseCodec extends AbstractCodec<MultiValuedQueryResponse>{
    @Override
    protected int getObjectId() {
        return 4;
    }

    @Override
    protected Class<MultiValuedQueryResponse> getObjectClass() {
        return MultiValuedQueryResponse.class;
    }
}
