/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.sdk.common.serial.schema;

import io.dingodb.sdk.common.serial.Buf;

import java.nio.charset.StandardCharsets;

public class ArraySchema<T> implements DingoSchema<T[]> {
    private int index;
    private boolean isKey;
    private boolean allowNull = true;
    private DingoSchema<T> elementSchema;

    public ArraySchema(DingoSchema<T> elementSchema) {
        this.elementSchema = elementSchema;
    }

    @Override
    public Type getType() {
        return Type.ARRAY;
    }

    @Override
    public void setIndex(int index) {
        this.index = index;
    }

    @Override
    public int getIndex() {
        return index;
    }

    @Override
    public void setIsKey(boolean isKey) {
        this.isKey = isKey;
    }

    @Override
    public boolean isKey() {

        return isKey;
    }

    @Override
    public int getValueLengthV2() {
        return 0;
    }

    @Override
    public int getWithNullTagLength() {
        return 1;
    }

    @Override
    public int getLength() {
        if (allowNull) {
            return elementSchema.getLength() + 1;
        }
        return elementSchema.getLength();
    }

    private int getLength(T[] data) {
        int sum = 0;
        int elementSchemaSize = 0;
        switch (elementSchema.getType()) {
            case BOOLEAN:
            case INTEGER:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BYTES:
                elementSchemaSize = elementSchema.getLength();
                if (allowNull) {
                    sum = 5 + elementSchemaSize * data.length;
                } else {
                    sum = 4 + elementSchemaSize * data.length;
                }
                break;
            case STRING:
                for (T value: data) {
                    byte[] bytes = ((String)value).getBytes(StandardCharsets.UTF_8);
                    sum += bytes.length;
                }
                if (allowNull) {
                    sum += 5;
                } else {
                    sum += 4;
                }

                break;
            default:
                break;
        }
        return sum;
    }

    private int getLengthV2(T[] data) {
        int sum = 0;
        int elementSchemaSize = 0;
        switch (elementSchema.getType()) {
            case BOOLEAN:
            case INTEGER:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BYTES:
                elementSchemaSize = elementSchema.getLength();
                sum = 4 + elementSchemaSize * data.length;
                break;
            case STRING:
                for (T value: data) {
                    byte[] bytes = ((String)value).getBytes(StandardCharsets.UTF_8);
                    sum += bytes.length;
                }
                sum += 4;
                break;
            default:
                break;
        }
        return sum;
    }

    @Override
    public void setAllowNull(boolean allowNull) {
        this.allowNull = allowNull;
    }

    @Override
    public boolean isAllowNull() {
        return allowNull;
    }

    @Override
    public void encodeKey(Buf buf, T[] data) {

        throw new RuntimeException("Array cannot be key");
    }

    @Override
    public void encodeKeyV2(Buf buf, T[] data) {

        throw new RuntimeException("Array cannot be key");
    }

    @Override
    public void encodeKeyForUpdate(Buf buf, T[] data) {

        throw new RuntimeException("Array cannot be key");
    }

    @Override
    public void encodeKeyForUpdateV2(Buf buf, T[] data) {

        throw new RuntimeException("Array cannot be key");
    }

    @Override
    public T[] decodeKey(Buf buf) {

        throw new RuntimeException("Array cannot be key");
    }

    @Override
    public T[] decodeKeyV2(Buf buf) {

        throw new RuntimeException("Array cannot be key");
    }

    @Override
    public T[] decodeKeyPrefix(Buf buf) {

        throw new RuntimeException("Array cannot be key");
    }

    @Override
    public void skipKey(Buf buf) {

        throw new RuntimeException("Array cannot be key");
    }

    @Override
    public void skipKeyV2(Buf buf) {

        throw new RuntimeException("Array cannot be key");
    }

    @Override
    public void encodeKeyPrefix(Buf buf, T[] data) {
        throw new RuntimeException("Array cannot be key");
    }

    @Override
    public void encodeValue(Buf buf, T[] data) {
        if (allowNull) {
            if (data == null) {
                buf.ensureRemainder(1);
                buf.write(NULL);
            } else {
                buf.ensureRemainder(getLength(data));
                buf.write(NOTNULL);
                buf.writeInt(data.length);
                for (T element : data) {
                    elementSchema.encodeValue(buf, element);
                }
            }
        } else {
            buf.ensureRemainder(getLength(data));
            buf.writeInt(data.length);
            for (T element : data) {
                elementSchema.encodeValue(buf, element);
            }
        }
    }

    @Override
    public int encodeValueV2(Buf buf, T[] data) {
        int len = 0;

        if (allowNull) {
            if (data == null) {
                return 0;
            } else {
                len = getLengthV2(data);
                buf.ensureRemainder(len);
                buf.writeInt(data.length);
                for (T element : data) {
                    elementSchema.encodeValue(buf, element);
                }
            }
        } else {
            len = getLengthV2(data);
            buf.ensureRemainder(len);
            buf.writeInt(data.length);
            for (T element : data) {
                elementSchema.encodeValue(buf, element);
            }
        }

        return len;
    }

    @Override
    public T[] decodeValue(Buf buf) {
        if (allowNull) {
            if (buf.read() == NULL) {
                return null;
            }
        }
        int length = buf.readInt();
        T[] array = (T[]) new Object[length];
        for (int i = 0; i < length; i++) {
            array[i] = elementSchema.decodeValue(buf);
        }
        return array;
    }

    @Override
    public T[] decodeValueV2(Buf buf) {
        int length = buf.readInt();
        T[] array = (T[]) new Object[length];
        for (int i = 0; i < length; i++) {
            array[i] = elementSchema.decodeValue(buf);
        }
        return array;
    }

    @Override
    public void skipValue(Buf buf) {
        if (allowNull) {
            if (buf.read() == NULL) {
                return;
            }
        }
        int length = buf.readInt();
        for (int i = 0; i < length; i++) {
            elementSchema.skipValue(buf);
        }
    }

    @Override
    public void skipValueV2(Buf buf) {
        int length = buf.readInt();
        for (int i = 0; i < length; i++) {
            elementSchema.skipValueV2(buf);
        }
    }
}