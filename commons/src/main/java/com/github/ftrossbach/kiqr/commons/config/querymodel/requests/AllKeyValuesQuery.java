package com.github.ftrossbach.kiqr.commons.config.querymodel.requests;

/**
 * Created by ftr on 20/02/2017.
 */
public class AllKeyValuesQuery {

    private String storeName;
    private String keySerde;
    private String valueSerde;


    public AllKeyValuesQuery() {
    }

    public AllKeyValuesQuery(String storeName, String keySerde, String valueSerde) {
        this.storeName = storeName;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
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

    public String getValueSerde() {
        return valueSerde;
    }

    public void setValueSerde(String valueSerde) {
        this.valueSerde = valueSerde;
    }
}
