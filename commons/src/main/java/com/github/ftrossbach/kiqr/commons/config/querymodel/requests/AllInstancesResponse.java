package com.github.ftrossbach.kiqr.commons.config.querymodel.requests;

import java.util.Set;

/**
 * Created by ftr on 22/02/2017.
 */
public class AllInstancesResponse {
    private Set<String> instances;


    public AllInstancesResponse() {
    }

    public AllInstancesResponse(Set<String> instances) {
        this.instances = instances;
    }

    public Set<String> getInstances() {
        return instances;
    }

    public void setInstances(Set<String> instances) {
        this.instances = instances;
    }
}
