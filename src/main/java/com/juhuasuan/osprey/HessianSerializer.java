package com.juhuasuan.osprey;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.caucho.hessian.io.HessianInput;
import com.caucho.hessian.io.HessianOutput;
import com.caucho.hessian.io.SerializerFactory;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2012-2-14 ����3:20:46
 * @version 1.0
 */
public class HessianSerializer {

    static private SerializerFactory _serializerFactory;
    static {
        _serializerFactory = new SerializerFactory();
    }

    private static HessianOutput createHessianOutput(OutputStream out) {
        HessianOutput hout = new HessianOutput(out);
        hout.setSerializerFactory(_serializerFactory);
        return hout;
    }

    private static HessianInput createHessianInput(InputStream in) {
        HessianInput hin = new HessianInput(in);
        hin.setSerializerFactory(_serializerFactory);
        return hin;
    }

    public Object deserialize(byte[] bytes) throws IOException {
        if (bytes == null) {
            return null;
        }
        ByteArrayInputStream input = new ByteArrayInputStream(bytes);
        HessianInput hin = createHessianInput(input);
        return hin.readObject();
    }

    public byte[] serialize(Object t) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        createHessianOutput(bout).writeObject(t);
        return bout.toByteArray();
    }

}
