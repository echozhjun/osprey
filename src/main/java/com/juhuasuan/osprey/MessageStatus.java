package com.juhuasuan.osprey;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2012-3-13 ÉÏÎç11:06:09
 * @version 1.0
 */
public final class MessageStatus {

    private boolean success = true;

    private boolean rollbackOnly = false;

    private String reason;

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public boolean isRollbackOnly() {
        return rollbackOnly;
    }

    public void setRollbackOnly() {
        this.rollbackOnly = true;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

}
