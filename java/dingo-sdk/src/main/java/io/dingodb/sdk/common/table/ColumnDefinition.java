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

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@Builder
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class ColumnDefinition implements Column {

    private String name;
    private String type;
    private String elementType;
    @Builder.Default
    private int precision = Column.DEFAULT_PRECISION;
    @Builder.Default
    private int scale = Column.DEFAULT_SCALE;
    @Builder.Default
    private boolean nullable = true;
    @Builder.Default
    private int primary = -1;
    private String defaultValue;
    private boolean isAutoIncrement;

    private int state = Column.DISABLE;
    private int createVersion;
    private int updateVersion;
    private int deleteVersion;

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
        boolean isAutoIncrement
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
    }

    @Override
    public String getName() {
        return name.toUpperCase();
    }

    @Override
    public int getPrimary() {
        return primary < 0 ? -1 : primary;
    }

}
