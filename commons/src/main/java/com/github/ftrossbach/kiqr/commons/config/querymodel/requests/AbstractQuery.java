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
package com.github.ftrossbach.kiqr.commons.config.querymodel.requests;

/**
 * Created by ftr on 20/02/2017.
 */
public abstract class AbstractQuery implements HasStoreName{

    private String storeName;
    private String keySerde;
    private String valueSerde;


    public AbstractQuery(){}

    public AbstractQuery(String storeName, String keySerde, String valueSerde) {
        this.storeName = storeName;
        this.keySerde = keySerde;

        this.valueSerde = valueSerde;
    }

    public String getStoreName() {
        return storeName;
    }

    public void setStoreName(String storeName) {
        this.storeName = storeName;
    }

    public String getKeySerde() {
        return keySerde;
    }

    public void setKeySerde(String keySerde) {
        this.keySerde = keySerde;
    }

    public String getValueSerde() {
        return valueSerde;
    }

    public void setValueSerde(String valueSerde) {
        this.valueSerde = valueSerde;
    }
}
