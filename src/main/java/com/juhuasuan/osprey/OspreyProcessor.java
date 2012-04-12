package com.juhuasuan.osprey;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2012-2-6 ����4:07:41
 * @version 1.0
 */
public interface OspreyProcessor<T extends Message> {
    /**
     * ��Processor���Դ����ҵ���������͡�
     * 
     * @return ��Processor���Դ����ҵ���������͡������Է���NULL��
     */
    Class<T> interest();

    /**
     * �÷�����������Զ�ҵ�������
     */
    Result process(T event);

    public interface OspreyPreProcessor<T extends Message> {
        void beforeProcess(T event);
    }

}
