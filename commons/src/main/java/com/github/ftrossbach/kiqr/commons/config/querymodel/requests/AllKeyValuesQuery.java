package com.github.ftrossbach.kiqr.commons.config.querymodel.requests;

/**
 * Created by ftr on 20/02/2017.
 */
public class AllKeyValuesQuery {

    private String storeName;

    public AllKeyValuesQuery() {
    }

    public AllKeyValuesQuery(String storeName) {
       this.storeName = storeName;
    }

    public String getStoreName() {
        return storeName;
    }

    public void setStoreName(String storeName) {
        this.storeName = storeName;
    }
}
