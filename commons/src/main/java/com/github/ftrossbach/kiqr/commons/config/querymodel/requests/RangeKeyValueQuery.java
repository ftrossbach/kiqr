package com.github.ftrossbach.kiqr.commons.config.querymodel.requests;

/**
 * Created by ftr on 20/02/2017.
 */
public class RangeKeyValueQuery {


    private String storeName;
    private String keySerde;
    private String valueSerde;
    private byte[] from;
    private byte[] to;

    public RangeKeyValueQuery() {
    }

    public RangeKeyValueQuery(String storeName, String keySerde, String valueSerde, byte[] from, byte[] to) {
        this.storeName = storeName;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.from = from;
        this.to = to;
    }

    public byte[] getFrom() {
        return from;
    }

    public void setFrom(byte[] from) {
        this.from = from;
    }

    public byte[] getTo() {
        return to;
    }

    public void setTo(byte[] to) {
        this.to = to;
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
