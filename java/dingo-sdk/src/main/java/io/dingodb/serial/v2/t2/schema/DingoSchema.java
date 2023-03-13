package io.dingodb.serial.v2.t2.schema;

import io.dingodb.serial.v1.schema.Type;
import io.dingodb.serial.v2.t2.Buf;
import io.dingodb.serial.v2.t2.ValueBuf;

public interface DingoSchema<T> {

    byte NULL = 0;
    byte NOTNULL = 1;

    Type getType();

    void setIndex(int index);

    int getIndex();

    void setIsKey(boolean isKey);

    boolean isKey();

    int getLength();

    void setAllowNull(boolean allowNull);

    boolean isAllowNull();

    void encodeKey(Buf buf, T data);
    T decodeKey(Buf buf);
    void skipKey(Buf buf);

    void encodeKeyPrefix(Buf buf, T data);

    void encodeValue(Buf buf, T data);
    void encodeValue(ValueBuf buf, T data);
    T decodeValue(ValueBuf buf);
    void skipValue(ValueBuf buf);

}
