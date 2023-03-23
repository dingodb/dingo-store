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

package io.dingodb.sdk.common.type.scalar;

import io.dingodb.sdk.common.serial.schema.DingoSchema;
import io.dingodb.sdk.common.type.NullType;
import io.dingodb.sdk.common.type.NullableType;
import io.dingodb.sdk.common.type.TypeCode;
import io.dingodb.sdk.common.type.converter.DataConverter;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public abstract class AbstractScalarType extends NullableType {
    protected AbstractScalarType(int typeCode, boolean nullable) {
        super(typeCode, nullable);
    }


    @Override
    public List<DingoSchema> toDingoSchemas() {
        return null;
    }

    @Override
    public @NonNull String format(@Nullable Object value) {
        return value != null ? value + ":" + this : NullType.NULL.format(null);
    }

    @Override
    protected Object convertValueTo(@NonNull Object value, @NonNull DataConverter converter) {
        return value;
    }

    @Override
    protected Object convertValueFrom(@NonNull Object value, @NonNull DataConverter converter) {
        return value;
    }

    @Override
    public int fieldCount() {
        return -1;
    }

    @Override
    public String toString() {
        String name = TypeCode.nameOf(typeCode);
        return nullable ? name + "|" + NullType.NULL : name;
    }

}
