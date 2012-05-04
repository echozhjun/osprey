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
 * @since 2012-3-16
 * @version 1.0
 */
public class Result {
    private boolean success = true;
    private Object model;
    private String errorMessage;
    private String messageId;
    private RuntimeException re;
    private ResultType sendResultType;

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public Object getModel() {
        return model;
    }

    public void setModel(Object model) {
        this.model = model;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public void setRuntimeException(RuntimeException re) {
        this.re = re;
    }

    public RuntimeException getRuntimeException() {
        return this.re;
    }

    public ResultType getSendResultType() {
        return sendResultType;
    }

    public void setSendResultType(ResultType sendResultType) {
        this.sendResultType = sendResultType;
    }

}
