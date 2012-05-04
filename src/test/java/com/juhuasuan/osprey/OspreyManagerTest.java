/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.juhuasuan.osprey;


/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2012-3-19
 * @version 1.0
 */
public class OspreyManagerTest {

    public static void main(String[] args) {
//        BasicConfigurator.configure();
        OspreyManager ospreyManager = new OspreyManager("osprey-test");
        ospreyManager.registerProcessor(new TestProcessor());
        ospreyManager.init();
        Message message = new TestMessage();
        Result result = ospreyManager.addMessage(message, false);
        System.out.println(result.isSuccess());
        result = ospreyManager.commitMessage(message, result);
        System.out.println(result.isSuccess());
    }

    public static class TestMessage extends Message {

        private static final long serialVersionUID = -9006052790210673532L;

    }

    public static class TestProcessor implements OspreyProcessor<TestMessage> {

        public Class<TestMessage> interest() {
            return TestMessage.class;
        }

        public Result process(TestMessage event) {
            System.out.println("Handled event" + event);
            Result result = new Result();
            result.setSuccess(false);
            return result;
        }

    }

}
