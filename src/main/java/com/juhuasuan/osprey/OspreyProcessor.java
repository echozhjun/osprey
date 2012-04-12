package com.juhuasuan.osprey;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2012-2-6 下午4:07:41
 * @version 1.0
 */
public interface OspreyProcessor<T extends Message> {
    /**
     * 该Processor可以处理的业务请求类型。
     * 
     * @return 该Processor可以处理的业务请求类型。不可以返回NULL。
     */
    Class<T> interest();

    /**
     * 该方法用来处理对端业务层请求
     */
    Result process(T event);

    public interface OspreyPreProcessor<T extends Message> {
        void beforeProcess(T event);
    }

}
