/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb.sdk.common.table;

import io.dingodb.sdk.common.type.DingoType;
import io.dingodb.sdk.common.type.DingoTypeFactory;

public interface Column {

    void setName(String name);

    String getName();

    void setType(String type);

    String getType();

    void setElementType(String elementType);

    String getElementType();

    void setPrecision(int precision);

    int getPrecision();

    void setScale(int scale);

    int getScale();

    void nullable(boolean nullable);

    boolean isNullable();

    void setPrimary(int primary);

    int getPrimary();

    void setDefaultValue(String defaultValue);

    String getDefaultValue();

    default DingoType getDingoType() {
        return DingoTypeFactory.fromName(getType(), getElementType(), isNullable());
    }

    default boolean isPrimary() {
        return getPrimary() > -1;
    }

}
