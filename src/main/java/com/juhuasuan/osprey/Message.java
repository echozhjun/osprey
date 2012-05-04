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

import com.juhuasuan.osprey.store.UniqId;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2012-3-16
 * @version 1.0
 */
public abstract class Message implements Serializable {
    private static final long serialVersionUID = -8548915719716251362L;

    private long lostCount = 0;
    private byte[] messageId = null;

    public Message() {
        this.setMessageId(UniqId.getInstance().getUniqIDHash());
    }

    public byte[] getMessageId() {
        return messageId;
    }

    public void setMessageId(byte[] messageId) {
        this.messageId = messageId;
    }

    public long getLostCount() {
        return lostCount;
    }

    public void incrementLostCount() {
        this.lostCount++;
    }

}
