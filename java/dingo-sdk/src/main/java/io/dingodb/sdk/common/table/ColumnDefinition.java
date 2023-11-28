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

package io.dingodb.sdk.common.table;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Setter;
import lombok.ToString;

@Builder
@ToString
@EqualsAndHashCode
public class ColumnDefinition implements Column {

    public static final int DEFAULT_PRECISION = -1;
    public static final int DEFAULT_SCALE = Integer.MIN_VALUE;

    private String name;
    private String type;
    private String elementType;
    @Builder.Default
    private int precision = DEFAULT_PRECISION;
    @Builder.Default
    private int scale = DEFAULT_SCALE;
    @Builder.Default
    private boolean nullable = true;
    @Builder.Default
    private int primary = -1;
    private String defaultValue;
    private boolean isAutoIncrement;
    @Builder.Default
    @Setter
    private int state = 1;

    @Setter
    private String comment;

    @Deprecated
    public ColumnDefinition(
        String name,
        String type,
        String elementType,
        int precision,
        int scale,
        boolean nullable,
        int primary,
        String defaultValue,
        boolean isAutoIncrement,
        int state,
        String comment
    ) {
        this.name = name;
        this.type = type;
        this.elementType = elementType;
        this.precision = precision;
        this.scale = scale;
        this.nullable = nullable;
        this.primary = primary;
        this.defaultValue = defaultValue;
        this.isAutoIncrement = isAutoIncrement;
        this.state = state;
        this.comment = comment;
    }

    @Override
    public String getName() {
        return name.toUpperCase();
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public String getElementType() {
        return elementType;
    }

    @Override
    public int getPrecision() {
        return precision;
    }

    @Override
    public int getScale() {
        return scale;
    }

    @Override
    public boolean isNullable() {
        return nullable;
    }

    @Override
    public int getPrimary() {
        return primary < 0 ? -1 : primary;
    }

    @Override
    public String getDefaultValue() {
        return defaultValue;
    }

    @Override
    public boolean isAutoIncrement() {
        return isAutoIncrement;
    }

    @Override
    public int getState() {
        return state;
    }

    @Override
    public String getComment() {
        return comment;
    }
}
