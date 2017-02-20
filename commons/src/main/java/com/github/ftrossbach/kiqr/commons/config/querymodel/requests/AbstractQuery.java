package com.github.ftrossbach.kiqr.commons.config.querymodel.requests;

/**
 * Created by ftr on 20/02/2017.
 */
public abstract class AbstractQuery {

    private  String storeName;
    private  String keySerde;
    private  byte[] key;


    public AbstractQuery(){}

    public AbstractQuery(String storeName, String keySerde, byte[] key) {
        this.storeName = storeName;
        this.keySerde = keySerde;
        this.key = key;
    }

    public String getStoreName() {
        return storeName;
    }

    public void setStoreName(String storeName) {
        this.storeName = storeName;
    }

    public String getKeySerde() {
        return keySerde;
    }

    public void setKeySerde(String keySerde) {
        this.keySerde = keySerde;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }
}
