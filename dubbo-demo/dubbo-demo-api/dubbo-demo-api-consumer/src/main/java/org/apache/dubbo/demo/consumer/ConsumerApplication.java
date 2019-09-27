/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.dubbo.demo.consumer;

import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ReferenceConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.demo.DemoService;

public class ConsumerApplication {
    /**
     * In order to make sure multicast registry works, need to specify '-Djava.net.preferIPv4Stack=true' before
     * launch the application
     */
    public static void main(String[] args) {
        RegistryConfig registry = new RegistryConfig("zookeeper://127.0.0.1:2181");

        ReferenceConfig<DemoService> reference = new ReferenceConfig<>();
        reference.setApplication(new ApplicationConfig("dubbo-demo-api-consumer"));
        reference.setRegistry(registry);
        reference.setCheck(true);
        reference.setInterface(DemoService.class);
//        reference.setVersion("1.2.1");

        DemoService service = reference.get();
        String message = service.sayHello("dubbo");
        System.out.println(message);
    }
}
