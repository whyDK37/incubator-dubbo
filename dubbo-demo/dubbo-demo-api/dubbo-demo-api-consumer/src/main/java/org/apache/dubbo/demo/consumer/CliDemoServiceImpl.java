package org.apache.dubbo.demo.consumer;

import org.apache.dubbo.demo.DemoService;

/**
 * @author why
 */
public class CliDemoServiceImpl implements DemoService {
    @Override
    public String sayHello(String name) {
        return "client service," + name;
    }
}
