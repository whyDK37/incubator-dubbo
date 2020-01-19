package org.apache.dubbo.demo;

import java.io.Serializable;

/**
 * @author why
 */
public interface DemoCallBack extends Serializable {
    void call(String string);
}
