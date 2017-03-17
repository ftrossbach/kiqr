/**
 * Copyright © 2017 Florian Troßbach (trossbach@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.ftrossbach.kiqr.core.query;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by ftr on 05/03/2017.
 */
public class SimpleSessionIterator implements KeyValueIterator<Windowed<Object>, Object> {


    public boolean closed = false;

    private final Iterator<KeyValue<Windowed<Object>, Object>> iterator;

    public SimpleSessionIterator(KeyValue<Windowed<Object>, Object>... values){

        iterator = Arrays.asList(values).iterator();

    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public Windowed<Object> peekNextKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public KeyValue<Windowed<Object>, Object> next() {
        return iterator.next();
    }
}
