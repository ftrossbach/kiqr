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



import java.util.HashMap;
import java.util.Map;

/**
 * Created by ftr on 20/02/2017.
 */
public class MultiValuedKeyValueQueryResponse extends AbstractQueryResponse{


    private Map<String,String> results = new HashMap<>();

    public MultiValuedKeyValueQueryResponse() {
    }

    public MultiValuedKeyValueQueryResponse(QueryStatus status, Map<String,String> results) {
        super(status);
        this.results = results;
    }

    public Map<String,String> getResults() {
        return results;
    }

    public void setResults(Map<String,String> results) {
        this.results = results;
    }

    public MultiValuedKeyValueQueryResponse merge(MultiValuedKeyValueQueryResponse other){

        Map<String, String> left = new HashMap<>(this.results);
        Map<String, String> right = new HashMap<>(other.results);
        left.putAll(right);

        return new MultiValuedKeyValueQueryResponse(QueryStatus.OK, left);
    }
}
