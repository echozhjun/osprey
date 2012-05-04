/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.juhuasuan.osprey;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2012-2-7
 * @version 1.0
 */
final public class ProcessorUtil {

    static private final Log LOGGER = LoggerInit.LOGGER;

    private volatile ConcurrentHashMap<Class<?>, OspreyProcessor<?>> processors;

    public ProcessorUtil() {
        this(null);
    }

    private ProcessorUtil(Map<Class<?>, OspreyProcessor<?>> _processors) {
        processors = new ConcurrentHashMap<Class<?>, OspreyProcessor<?>>();
        if (null != _processors) {
            for (OspreyProcessor<?> p : _processors.values()) {
                registerProcessor(p);
            }
        }
    }

    public void registerProcessor(OspreyProcessor<?> processor) {
        if (null != processors.putIfAbsent(processor.interest(), processor)) {
            LOGGER.warn("Register Processor[" + processor.interest() + ", " + processor + "].", new Exception());
        }
    }

    public OspreyProcessor<?> removeProcessor(Class<?> eventClazz) {
        return (OspreyProcessor<?>) processors.remove(eventClazz);
    }

    public void updateProcessors(Map<Class<?>, OspreyProcessor<?>> newProcessors) {
        processors = new ProcessorUtil(newProcessors).processors;
    }

    public Map<Class<?>, OspreyProcessor<?>> getProcessors() {
        return processors;
    }

    public OspreyProcessor<?> findProcessor(Class<?> eventClazz) {
        return (OspreyProcessor<?>) processors.get(eventClazz);
    }

}
