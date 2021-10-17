/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.colloh.flink.kudu.connector.internal.failure;

    import org.apache.kudu.client.Bytes;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.RowError;
import org.apache.kudu.client.Status;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Default failure handling logic that doesn't do any handling but throws
 * an error.
 */
public class DefaultKuduFailureHandler implements KuduFailureHandler {

    @Override
    public void onFailure(List<RowError> failure) throws IOException {

        String errors = failure.stream()
                .map(error -> printFail(error) + System.lineSeparator())
                .collect(Collectors.joining());

        throw new IOException("Error while sending value. \n " + errors);
    }

    public String  printFail(RowError rowError){
        Operation operation = rowError.getOperation();
        return "-- Row error for primary key=" + Bytes.pretty(operation.getRow().encodePrimaryKey()) +
                ", operation class: " + operation.getClass().getName() +
                ", server=" + rowError.getTsUUID() +
                ", status=" + rowError.getErrorStatus() +
                ", data=" + operation.getRow().toString()
                ;
    }

}
