package com.juhuasuan.osprey.cache;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2012-3-15 обнГ4:35:44
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
