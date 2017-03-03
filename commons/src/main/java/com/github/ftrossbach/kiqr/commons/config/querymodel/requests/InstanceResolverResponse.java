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

import java.util.Optional;

/**
 * Created by ftr on 20/02/2017.
 */
public class InstanceResolverResponse extends AbstractQueryResponse{

    private Optional<String> instanceId;

    public InstanceResolverResponse() {
    }

    public InstanceResolverResponse(QueryStatus status, Optional<String> instanceId) {
        super(status);
        this.instanceId = instanceId;
    }

    public Optional<String> getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(Optional<String> instanceId) {
        this.instanceId = instanceId;
    }
}
