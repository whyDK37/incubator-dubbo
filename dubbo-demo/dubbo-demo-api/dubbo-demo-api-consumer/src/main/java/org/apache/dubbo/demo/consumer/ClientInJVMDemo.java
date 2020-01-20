package org.apache.dubbo.demo.consumer;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.demo.DemoService;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.protocol.dubbo.DubboProtocol;

/**
 * @author why
 */
public class ClientInJVMDemo {
    private static ProxyFactory proxy = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();
    public static void main(String[] args) {

        DubboProtocol protocol = DubboProtocol.getDubboProtocol();
        DemoService cliSer = new CliDemoServiceImpl();
        URL url = URL.valueOf("injvm://127.0.0.1/org.apache.dubbo.demo.DemoService")
                .addParameter(Constants.INTERFACE_KEY, DemoService.class.getName())
                .addParameter(Constants.IS_CALLBACK_SERVICE, Boolean.TRUE)
                .addParameter(Constants.IS_SERVER_KEY, Boolean.FALSE);
        Exporter<?> exporter = protocol.export(proxy.getInvoker(cliSer, DemoService.class, url));


        RpcInvocation message = new RpcInvocation();
        message.setMethodName("sayHello");
        message.setArguments(new Object[]{"call from server"});
        message.setParameterTypes(new Class[]{String.class});
        message.setAttachment(Constants.SIDE_KEY, Constants.PROVIDER_SIDE);
        message.setAttachment(Constants.INTERFACE_KEY, "org.apache.dubbo.demo.DemoService");
        message.setAttachment(Constants.PATH_KEY, "org.apache.dubbo.demo.DemoService");
        message.setAttachment(Constants.CALLBACK_SERVICE_KEY, "0");
        Result invoke = exporter.getInvoker().invoke(message);
        System.out.println("invoke.getValue() = " + invoke.getValue());
    }
}
