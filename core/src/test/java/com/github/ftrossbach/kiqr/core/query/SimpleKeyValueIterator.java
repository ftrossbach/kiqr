package com.github.ftrossbach.kiqr.core.query;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by ftr on 05/03/2017.
 */
public class SimpleKeyValueIterator implements KeyValueIterator<Object, Object> {


    public boolean closed = false;

    private final Iterator<KeyValue<Object, Object>> iterator;

    public SimpleKeyValueIterator(KeyValue<Object, Object>... values){

        iterator = Arrays.asList(values).iterator();

    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public Object peekNextKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public KeyValue<Object, Object> next() {
        return iterator.next();
    }
}
