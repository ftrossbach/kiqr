package com.github.ftrossbach.kiqr.client.service.rest;

import com.github.ftrossbach.kiqr.client.service.BlockingKiqrService;
import org.apache.kafka.common.serialization.Serdes;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.Map;
import java.util.Optional;

/**
 * Created by ftr on 01/03/2017.
 */
public class Main {

    public static void main(String[] args) {
        BlockingKiqrService service = new BlockingRestKiqrServiceImpl("localhost", 2901);

        System.out.println(service.getScalarKeyValue("visitStore", String.class, "127.0.0.1", Long.class, Serdes.String(), Serdes.Long()));

        System.out.println(service.getAllKeyValues("visitStore", String.class, Long.class, Serdes.String(), Serdes.Long()));
        System.out.println(service.getRangeKeyValues("visitStore", String.class, Long.class, Serdes.String(), Serdes.Long(), "127.0.0.2", "127.0.0.5"));
        Map<Long, Long> visitCount = service.getWindow("visitCount", String.class, "127.0.0.1", Long.class, Serdes.String(), Serdes.Long(), System.currentTimeMillis() - 1000 * 60 * 60, System.currentTimeMillis());
        System.out.println(visitCount);

        visitCount.forEach((time, val) -> System.out.println(
                LocalDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneId.systemDefault())));
    }
}
