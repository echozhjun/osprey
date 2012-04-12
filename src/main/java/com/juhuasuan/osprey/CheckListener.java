package com.juhuasuan.osprey;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2012-3-19 ÏÂÎç3:57:46
 * @version 1.0
 */
public interface CheckListener {

    void receiveCheckMessage(Message message, MessageStatus status);

}
