package com.github.ftrossbach.kiqr.commons.config.querymodel.requests;

/**
 * Created by ftr on 20/02/2017.
 */
public class RangeKeyValueQuery extends AbstractQuery {

    private byte[] from;
    private byte[] to;

    public RangeKeyValueQuery() {}
    public RangeKeyValueQuery(String storeName, String keySerde, byte[] key, byte[] from, byte[] to) {
        super(storeName, keySerde, key);
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
}
