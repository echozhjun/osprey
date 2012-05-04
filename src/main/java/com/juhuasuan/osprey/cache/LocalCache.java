/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.juhuasuan.osprey.cache;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2012-3-15
 * @version 1.0
 */
public interface LocalCache<K, V> {

    long getCurrentCacheSize();

    void setLowWaterMark(int lowWaterMark);

    void setHighWaterMark(int highWaterMark);

    int getLowWaterMark();

    int getHighWaterMark();

    V put(K key, V value);

    V get(Object key);

    V remove(Object key);

    void clear();
}
