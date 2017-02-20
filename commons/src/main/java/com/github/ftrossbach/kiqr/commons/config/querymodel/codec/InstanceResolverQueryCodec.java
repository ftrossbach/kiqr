package com.github.ftrossbach.kiqr.commons.config.querymodel.codec;

import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.InstanceResolverQuery;

/**
 * Created by ftr on 20/02/2017.
 */
public class InstanceResolverQueryCodec extends AbstractCodec<InstanceResolverQuery>{

    @Override
    protected int getObjectId() {
        return 0;
    }

    @Override
    protected Class<InstanceResolverQuery> getObjectClass() {
        return InstanceResolverQuery.class;
    }
}
