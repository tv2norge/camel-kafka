/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing permissions and limitations under the License.
 */
package org.giwi.camel.kafka.test;

import java.util.Properties;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;

/**
 *
 * @author ars
 */
public class EmbeddedKafka {

    private KafkaServer kafkaServer;
    
    public void start() {
        Properties props = new Properties();
        props.put("brokerid", "0");
        props.put("port", "9092");
        props.put("zk.connect", "localhost:2181");
        props.put("log.dir", "target/testKafka");
        KafkaConfig kafkaConfig = new KafkaConfig(props);
        kafkaServer = new KafkaServer(kafkaConfig);
        kafkaServer.startup();
        
    }
    public void stop() {
        kafkaServer.shutdown();
    }
    
}
