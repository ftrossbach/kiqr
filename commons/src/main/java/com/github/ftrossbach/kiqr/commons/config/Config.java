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
package com.github.ftrossbach.kiqr.commons.config;

/**
 * Created by ftr on 19/02/2017.
 */
public class Config {



    public static String INSTANCE_RESOLVER_ADDRESS_SINGLE = "com.github.ftrossbach.kiqr.core.resolver.single";
    public static String ALL_INSTANCES = "com.github.ftrossbach.kiqr.core.resolver.all";

    public static String KEY_VALUE_QUERY_ADDRESS_PREFIX = "com.github.ftrossbach.kiqr.core.query.kv.";
    public static String ALL_KEY_VALUE_QUERY_ADDRESS_PREFIX = "com.github.ftrossbach.kiqr.core.query.all_kv.";
    public static String RANGE_KEY_VALUE_QUERY_ADDRESS_PREFIX = "com.github.ftrossbach.kiqr.core.query.range_kv.";
    public static String COUNT_KEY_VALUE_QUERY_ADDRESS_PREFIX = "com.github.ftrossbach.kiqr.core.query.count_kv.";
    public static String WINDOWED_QUERY_ADDRESS_PREFIX = "com.github.ftrossbach.kiqr.core.query.window.";

    public static String KEY_VALUE_QUERY_FACADE_ADDRESS = "com.github.ftrossbach.kiqr.core.query.facade.kv";
    public static String ALL_KEY_VALUE_QUERY_FACADE_ADDRESS = "com.github.ftrossbach.kiqr.core.query.facade.all_kv";
    public static String RANGE_KEY_VALUE_QUERY_FACADE_ADDRESS = "com.github.ftrossbach.kiqr.core.query.facade.range_kv";
    public static String COUNT_KEY_VALUE_QUERY_FACADE_ADDRESS = "com.github.ftrossbach.kiqr.core.query.facade.count";
    public static String WINDOWED_QUERY_FACADE_ADDRESS = "com.github.ftrossbach.kiqr.core.query.facade.window";

    public static String CLUSTER_STATE_BROADCAST_ADDRESS = "com.github.ftrossbach.kiqr.core.cluster.state";
}
