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

public class VectorSchema implements DingoSchema<String> {

    private int index;
    private boolean isKey = false;
    private boolean allowNull = true;

    public VectorSchema() {
    }

    public VectorSchema(int index) {
        this.index = index;
    }

    @Override
    public Type getType() {
        return Type.VECTOR;
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
        if (isKey) {
            throw new RuntimeException("Vector cannot be key");
        }
    }

    @Override
    public boolean isKey() {
        return isKey;
    }

    @Override
    public int getLength() {
        return 0;
    }

    @Override
    public void setAllowNull(boolean allowNull) {
        this.allowNull = allowNull;
    }

    @Override
    public boolean isAllowNull() {
        return allowNull;
    }

    public void encodeKey(Buf buf, String data) {
        throw new RuntimeException("Vector cannot be key");
    }

    @Override
    public void encodeKeyForUpdate(Buf buf, String data) {
        throw new RuntimeException("Vector cannot be key");
    }

    @Override
    public String decodeKey(Buf buf) {
        throw new RuntimeException("Vector cannot be key");
    }

    @Override
    public String decodeKeyPrefix(Buf buf) {
        throw new RuntimeException("Vector cannot be key");
    }

    @Override
    public void skipKey(Buf buf) {
        throw new RuntimeException("Vector cannot be key");
    }

    @Override
    public void encodeKeyPrefix(Buf buf, String data) {
        throw new RuntimeException("Vector cannot be key");
    }

    @Override
    public void encodeValue(Buf buf, String data) {
        if (allowNull) {
            if (data == null) {
                buf.ensureRemainder(1);
                buf.write(NULL);
            } else {
                byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
                buf.ensureRemainder(1 + 4 + bytes.length);
                buf.write(NOTNULL);
                buf.writeInt(bytes.length);
                buf.write(bytes);
            }
        } else {
            byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
            buf.ensureRemainder(4 + bytes.length);
            buf.writeInt(bytes.length);
            buf.write(bytes);
        }
    }

    @Override
    public String decodeValue(Buf buf) {
        if (allowNull) {
            if (buf.read() == NULL) {
                return null;
            } else {
                return new String(buf.read(buf.readInt()), StandardCharsets.UTF_8);
            }
        }
        return new String(buf.read(buf.readInt()), StandardCharsets.UTF_8);
    }

    @Override
    public void skipValue(Buf buf) {
        if (allowNull) {
            if (buf.read() == NOTNULL) {
                buf.skip(buf.readInt());
            }
        } else {
            buf.skip(buf.readInt());
        }
    }
}
