package com.github.ftrossbach.kiqr.commons.config.querymodel.requests;

/**
 * Created by ftr on 16/03/2017.
 */
public class KeyValueStoreCountQuery implements HasStoreName{

    private String storeName;

    public KeyValueStoreCountQuery(String storeName) {
        this.storeName = storeName;
    }

    public KeyValueStoreCountQuery() {
    }

    public void setStoreName(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public String getStoreName() {
        return storeName;
    }
}
