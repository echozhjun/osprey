package com.juhuasuan.osprey;

import java.io.Serializable;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2012-3-15 ÏÂÎç4:57:34
 * @version 1.0
 */
public class MessageInStore4j implements Serializable {
    private static final long serialVersionUID = -4444632901328933770L;
    private Message message = null;
    private boolean enabledSend = true;
    private long createTime;

    public MessageInStore4j(Message message, boolean enabledSend) {
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
