package com.github.ftrossbach.kiqr.commons.config.querymodel.requests;

/**
 * Created by ftr on 20/02/2017.
 */
public class WindowedQuery extends AbstractQuery{

    private String valueSerde;
    private long from;
    private long to;


    public WindowedQuery(){}

    public WindowedQuery(String storeName, String keySerde, byte[] key, String valueSerde, long from, long to) {
        super(storeName, keySerde, key);
        this.valueSerde = valueSerde;
        this.from = from;
        this.to = to;
    }

    public String getValueSerde() {
        return valueSerde;
    }

    public void setValueSerde(String valueSerde) {
        this.valueSerde = valueSerde;
    }

    public long getFrom() {
        return from;
    }

    public void setFrom(long from) {
        this.from = from;
    }

    public long getTo() {
        return to;
    }

    public void setTo(long to) {
        this.to = to;
    }
}
