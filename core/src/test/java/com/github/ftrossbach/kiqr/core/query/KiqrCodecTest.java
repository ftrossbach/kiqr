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

import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.ScalarKeyValueQuery;
import io.vertx.core.buffer.Buffer;
import org.junit.Before;
import org.junit.Test;


import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

/**
 * Created by ftr on 06/03/2017.
 */
public class KiqrCodecTest {

    private KiqrCodec<ScalarKeyValueQuery> uut;

    @Before
    public void setUp(){
        uut = new KiqrCodec<>(ScalarKeyValueQuery.class);
    }

    @Test
    public void decodeEncode(){

        Buffer buffer = Buffer.buffer();
        uut.encodeToWire(buffer, new ScalarKeyValueQuery("store", "key serde", "key".getBytes(), "value serde"));

        ScalarKeyValueQuery deserializedResult = uut.decodeFromWire(0, buffer);

        assertThat(deserializedResult.getStoreName(), is(equalTo("store")));
        assertThat(deserializedResult.getKeySerde(), is(equalTo("key serde")));
        assertThat(new String(deserializedResult.getKey()), is(equalTo("key")));
        assertThat(deserializedResult.getValueSerde(), is(equalTo("value serde")));

    }

    @Test
    public void passThrough(){

        ScalarKeyValueQuery result = uut.transform(new ScalarKeyValueQuery("store", "key serde", "key".getBytes(), "value serde"));
        assertThat(result.getStoreName(), is(equalTo("store")));
        assertThat(result.getKeySerde(), is(equalTo("key serde")));
        assertThat(new String(result.getKey()), is(equalTo("key")));
        assertThat(result.getValueSerde(), is(equalTo("value serde")));
    }

    @Test
    public void id(){

        byte b = uut.systemCodecID();

        assertThat(b, is(equalTo((byte) -1)));
    }
}
