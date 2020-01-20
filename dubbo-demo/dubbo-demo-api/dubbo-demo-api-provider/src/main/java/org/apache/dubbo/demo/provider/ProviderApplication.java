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
package org.apache.dubbo.demo.provider;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.ServiceConfig;
import org.apache.dubbo.demo.DemoService;
import org.apache.dubbo.remoting.exchange.ExchangeChannel;
import org.apache.dubbo.remoting.exchange.ExchangeServer;
import org.apache.dubbo.remoting.exchange.ResponseFuture;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.protocol.dubbo.DubboProtocol;

import java.util.Collection;
import java.util.Scanner;

import static org.apache.dubbo.demo.provider.DemoServiceImpl.remoteInvoker;

public class ProviderApplication {
    /**
     * In order to make sure multicast registry works, need to specify '-Djava.net.preferIPv4Stack=true' before
     * launch the application
     */
    public static void main(String[] args) throws Exception {

        ServiceConfig<DemoServiceImpl> service = new ServiceConfig<>();
        service.setApplication(new ApplicationConfig("dubbo-demo-api-provider"));
        service.setRegistry(new RegistryConfig("zookeeper://127.0.0.1:2181"));
        service.setProtocol(new ProtocolConfig("dubbo", 12345));
        service.setInterface(DemoService.class);

//        MethodConfig methodConfig = new MethodConfig();
//        methodConfig.setName("sayHello");
//        ArgumentConfig argumentConfig = new ArgumentConfig();
//        argumentConfig.setIndex(1);
//        argumentConfig.setCallback(true);
//        methodConfig.setArguments(Lists.newArrayList(argumentConfig));
//        service.setMethods(Lists.newArrayList(methodConfig));
        service.setRef(new DemoServiceImpl());
        service.setVersion("1.2.1");
        service.export();
//        System.in.read();

        Scanner scanner = new Scanner(System.in);
        String input;
        while ((input = scanner.next()) != null) {
            if ("ls".equals(input)) {
                Collection<ExchangeServer> servers = DubboProtocol.getDubboProtocol().getServers();
                for (ExchangeServer server : servers) {
                    System.out.println("server.getLocalAddress() = " + server.getLocalAddress());
                    for (ExchangeChannel exchangeChannel : server.getExchangeChannels()) {
                        System.out.println("exchangeChannel.getRemoteAddress() = " + exchangeChannel.getRemoteAddress());
                    }
                }

            } else if ("iv".equals(input)) {
                remoteInvoker.forEach((s, invoker) -> System.out.println("remote:" + s + ", invoker:" + invoker));
            } else if ("send".equals(input)) {
                try {
                    ExchangeServer next = DubboProtocol.getDubboProtocol().getServers().iterator().next();
                    ExchangeChannel exchangeChannel = next.getExchangeChannels().iterator().next();
                    System.out.println("exchangeChannel.getClass() = " + exchangeChannel.getClass());
                    RpcInvocation invocation = new RpcInvocation();
                    invocation.setMethodName("sayHello");
                    invocation.setArguments(new Object[]{"call from server"});
                    invocation.setParameterTypes(new Class[]{String.class});
                    invocation.setAttachment(Constants.SIDE_KEY, Constants.PROVIDER_SIDE);
                    invocation.setAttachment(Constants.INTERFACE_KEY, "org.apache.dubbo.demo.DemoService");
                    invocation.setAttachment(Constants.PATH_KEY, "org.apache.dubbo.demo.DemoService");
                    invocation.setAttachment(Constants.CALLBACK_SERVICE_KEY, "0");
                    invocation.setAttachment(Constants.METHODS_KEY, "sayHello");
                    invocation.setAttachment(Constants.METHOD_KEY, "sayHello");
                    invocation.setAttachment("sayHello." + 0 + ".callback", "true");
                    ResponseFuture resp = exchangeChannel.request(invocation, Integer.MAX_VALUE);
                    System.out.println("request.get() = " + resp.get());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else if ("q".equalsIgnoreCase(input)) {
                System.out.println("exist...");
                break;
            } else {
                System.out.println("unsupported command");
            }
        }
    }
}
