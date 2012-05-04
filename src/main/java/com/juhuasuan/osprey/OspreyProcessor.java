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
 * @since 2012-2-6
 * @version 1.0
 */
public interface OspreyProcessor<T extends Message> {

    Class<T> interest();

    Result process(T event);

    public interface OspreyPreProcessor<T extends Message> {
        void beforeProcess(T event);
    }

}
