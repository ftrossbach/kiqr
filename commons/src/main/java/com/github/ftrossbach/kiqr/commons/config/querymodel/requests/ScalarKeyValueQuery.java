package com.github.ftrossbach.kiqr.commons.config.querymodel.requests;

/**
 * Created by ftr on 20/02/2017.
 */
public class ScalarKeyValueQuery extends AbstractQuery{

    private String valueSerde;


    public ScalarKeyValueQuery(){}

    public ScalarKeyValueQuery(String storeName, String keySerde, byte[] key, String valueSerde) {
        super(storeName, keySerde, key);
        this.valueSerde = valueSerde;
    }

    public String getValueSerde() {
        return valueSerde;
    }

    public void setValueSerde(String valueSerde) {
        this.valueSerde = valueSerde;
    }
}
