package com.github.ftrossbach.kiqr.core.query;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.WindowStoreIterator;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by ftr on 05/03/2017.
 */
public class SimpleWindowStoreIterator implements WindowStoreIterator<Object>{

    private final Iterator<KeyValue<Long, Object>> iterator;
    public boolean closed = false;

    public SimpleWindowStoreIterator(KeyValue<Long, Object>... values){
        iterator = Arrays.asList(values).iterator();
    }

    @Override
    public void close() {
      closed = true;
    }

    @Override
    public Long peekNextKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public KeyValue<Long, Object> next() {
        return iterator.next();
    }
}
