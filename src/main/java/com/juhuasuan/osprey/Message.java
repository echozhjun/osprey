package com.juhuasuan.osprey;

import java.io.Serializable;

import com.taobao.common.store.util.UniqId;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2012-3-16 ÏÂÎç4:26:02
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
