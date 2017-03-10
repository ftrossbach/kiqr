package com.github.ftrossbach.kiqr.client.service.rest;

import com.github.ftrossbach.kiqr.client.service.GenericBlockingKiqrClient;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


import static org.mockito.Mockito.*;
import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

/**
 * Created by ftr on 10/03/2017.
 */
public class SpecificBlockingRestKiqrClientImplTest {

    @Mock
    private GenericBlockingKiqrClient clientMock;

    private SpecificBlockingRestKiqrClientImpl<String, Long> unitUnderTest;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        unitUnderTest = new SpecificBlockingRestKiqrClientImpl<>(clientMock, "store", String.class, Long.class, Serdes.String(), Serdes.Long());

    }


    @Test
    public void scalar(){

        when(clientMock.getScalarKeyValue(any(), any(), any(), any(), any(), any())).thenReturn(Optional.of(6L));

        Optional<Long> result = unitUnderTest.getScalarKeyValue("key1");

        assertTrue(result.isPresent());
        assertThat(result.get(), is(equalTo(6L)));



    }

    @Test
    public void all(){

        Map<String, Long> results = new HashMap<>();
        results.put("key1", 6L);
        when(clientMock.getAllKeyValues(any(), eq(String.class), eq(Long.class), any(), any())).thenReturn(results);

        Map<String, Long> result = unitUnderTest.getAllKeyValues();

        assertThat(result.size(), is(equalTo(1)));
        assertThat(result, hasEntry("key1", 6L));

    }


    @Test
    public void range(){

        Map<String, Long> results = new HashMap<>();
        results.put("key1", 6L);
        when(clientMock.getRangeKeyValues(any(), eq(String.class), eq(Long.class), any(), any(), any(), any())).thenReturn(results);

        Map<String, Long> result = unitUnderTest.getRangeKeyValues("key1", "key2");

        assertThat(result.size(), is(equalTo(1)));
        assertThat(result, hasEntry("key1", 6L));

    }

    @Test
    public void window(){

        Map<Long, Long> results = new HashMap<>();
        results.put(1L, 6L);
        when(clientMock.getWindow(anyString(), eq(String.class), eq("key1"), eq(Long.class), anyObject(), anyObject(), eq(0L), eq(1L))).thenReturn(results);

        Map<Long, Long> result = unitUnderTest.getWindow("key1", 0L, 1L);

        assertThat(result.size(), is(equalTo(1)));
        assertThat(result, hasEntry(1L, 6L));

    }
}
