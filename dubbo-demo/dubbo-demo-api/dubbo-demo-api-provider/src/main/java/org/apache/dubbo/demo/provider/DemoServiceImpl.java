/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.demo.provider;

import org.apache.dubbo.demo.DemoCallBack;
import org.apache.dubbo.demo.DemoService;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DemoServiceImpl implements DemoService {
    private static final Logger logger = LoggerFactory.getLogger(DemoServiceImpl.class);

    public static Map<String, Invoker<?>> remoteInvoker = new ConcurrentHashMap<>();

    @Override
    public String sayHello(String name, DemoCallBack callBack) {
        InetSocketAddress remoteKey = RpcContext.getContext().getRemoteAddress();
        logger.info("Hello " + name + ", request from consumer: " + remoteKey);
        Invoker<?> invoker = RpcContext.getContext().getInvoker();
        remoteInvoker.put(remoteKey.toString(), invoker);
        callBack.call("call back from server");
        return "Hello " + name + ", response from provider: " + RpcContext.getContext().getLocalAddress();

    }

}
