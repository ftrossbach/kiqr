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
