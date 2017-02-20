package com.github.ftrossbach.kiqr.commons.config.querymodel.requests;

import java.util.Optional;

/**
 * Created by ftr on 20/02/2017.
 */
public class InstanceResolverResponse extends AbstractQueryResponse{

    private Optional<String> instanceId;

    public InstanceResolverResponse() {
    }

    public InstanceResolverResponse(QueryStatus status, Optional<String> instanceId) {
        super(status);
        this.instanceId = instanceId;
    }

    public Optional<String> getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(Optional<String> instanceId) {
        this.instanceId = instanceId;
    }
}
