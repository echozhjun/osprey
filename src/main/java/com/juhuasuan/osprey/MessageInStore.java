/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.juhuasuan.osprey;

import java.io.Serializable;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2012-3-15
 * @version 1.0
 */
public class MessageInStore implements Serializable {
    private static final long serialVersionUID = -4444632901328933770L;
    private Message message = null;
    private boolean enabledSend = true;
    private long createTime;

    public MessageInStore(Message message, boolean enabledSend) {
        this.message = message;
        this.enabledSend = enabledSend;
        this.createTime = System.currentTimeMillis();
    }

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }

    public boolean isEnabledSend() {
        return enabledSend;
    }

    public void setEnabledSend(boolean enabledSend) {
        this.enabledSend = enabledSend;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

}
