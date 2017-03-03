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
package com.github.ftrossbach.kiqr.examples;

import com.github.ftrossbach.kiqr.client.service.BlockingKiqrService;
import com.github.ftrossbach.kiqr.client.service.rest.BlockingRestKiqrServiceImpl;
import org.apache.kafka.common.serialization.Serdes;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;

/**
 * Created by ftr on 01/03/2017.
 */
public class RestClient {

    public static void main(String[] args) {
        BlockingKiqrService service = new BlockingRestKiqrServiceImpl("localhost", 2901);

        System.out.println(service.getScalarKeyValue("visitStore", String.class, "127.0.0.1", Long.class, Serdes.String(), Serdes.Long()));

        System.out.println(service.getAllKeyValues("visitStores", String.class, Long.class, Serdes.String(), Serdes.Long()));
        System.out.println(service.getRangeKeyValues("visitStore", String.class, Long.class, Serdes.String(), Serdes.Long(), "127.0.0.2", "127.0.0.5"));
        Map<Long, Long> visitCount = service.getWindow("visitCount", String.class, "127.0.0.1", Long.class, Serdes.String(), Serdes.Long(), System.currentTimeMillis() - 1000 * 60 * 60, System.currentTimeMillis());
        System.out.println(visitCount);

        visitCount.forEach((time, val) -> System.out.println(
                LocalDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneId.systemDefault())));
    }
}
