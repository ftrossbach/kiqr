package com.github.ftrossbach.kiqr.commons.config.querymodel.codec;


import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.InstanceResolverResponse;

/**
 * Created by ftr on 20/02/2017.
 */
public class InstanceResolverResponseCodec extends AbstractCodec<InstanceResolverResponse>{
    @Override
    protected int getObjectId() {
        return 6;
    }

    @Override
    protected Class<InstanceResolverResponse> getObjectClass() {
        return InstanceResolverResponse.class;
    }
}
