package org.janusgraph.graphdb.database.serialize;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.TaggedFieldSerializer;
import org.janusgraph.kydsj.serialize.MediaData;
import org.janusgraph.kydsj.serialize.MediaDataRaw;
import org.janusgraph.kydsj.serialize.Note;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;

/**
 * kryo 对象是一个线程不安全对象,不允许多线程共享.
 */
public class KryoSerializationUtil {
    private static final ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            kryo.setReferences(false);
            kryo.setRegistrationRequired(false);
            kryo.register(MediaData.class);
            kryo.register(MediaDataRaw.class);
            kryo.register(Note.class);
            kryo.setDefaultSerializer(TaggedFieldSerializer.class);
            return kryo;
        };
    };

    /**
     * 序列化
     */
    public static <T extends Serializable> byte[] serializable(T t) throws IOException{
       /* Output output = new Output(128, 10240);
        kryos.get().writeClassAndObject(output,t);
        output.flush();
        byte[] array = output.toBytes();
        output.close();
        return array;*/
        try(ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            Output output = new Output(baos);
            kryos.get().writeClassAndObject(output, t);
            output.flush();
            output.close();
            byte[] array = baos.toByteArray();
            baos.flush();
            return array;
        }
    }

    /**
     * 反序列化
     */
    public static<T>T deserialization(byte[] array,Class<T> cls) {
        Input input=new Input(array);
        T t=(T)kryos.get().readClassAndObject(input);
        input.close();
        return t;
    }
}
