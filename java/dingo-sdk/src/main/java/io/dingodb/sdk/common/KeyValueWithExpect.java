package io.dingodb.sdk.common;

import lombok.Getter;

import java.util.Arrays;

@Getter
public class KeyValueWithExpect extends KeyValue {

    public final byte[] expect;

    public KeyValueWithExpect(byte[] primaryKey, byte[] raw, byte[] expect) {
        super(primaryKey, raw);
        this.expect = expect;
    }

    public KeyValueWithExpect(KeyValue keyValue, byte[] expect) {
        this(keyValue.getKey(), keyValue.getValue(), expect);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        KeyValueWithExpect that = (KeyValueWithExpect) o;
        return Arrays.equals(primaryKey, that.primaryKey);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(primaryKey);
    }
}
