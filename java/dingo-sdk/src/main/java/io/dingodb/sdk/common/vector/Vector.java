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

package io.dingodb.sdk.common.vector;

import lombok.Getter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

@Getter
@ToString
public class Vector {

    private int dimension;
    private ValueType valueType;
    private List<Float> floatValues;
    private List<byte[]> binaryValues;

    public Vector() {
    }

    public Vector(int dimension, ValueType valueType, List<Float> floatValues, List<byte[]> binaryValues) {
        this.dimension = dimension;
        this.valueType = valueType;
        this.floatValues = floatValues;
        this.binaryValues = binaryValues;
    }

    public enum ValueType {
        FLOAT,
        BINARY
    }

    public static Vector getFloatInstance(int dimension, List<Float> values) {
        return new Vector(dimension, ValueType.FLOAT, values, new ArrayList<>());
    }

    public static Vector getBinaryInstance(int dimension, List<byte[]> values) {
        return new Vector(dimension, ValueType.BINARY, new ArrayList<>(), values);
    }
}
