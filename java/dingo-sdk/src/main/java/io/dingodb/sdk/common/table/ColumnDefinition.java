/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb.sdk.common.table;

public class ColumnDefinition implements Column {

    private String name;
    private String type;
    private String elementType;
    private int precision;
    private int scale;
    private boolean nullable = true;
    private int primary;
    private String defaultValue;

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public void setElementType(String elementType) {
        this.elementType = elementType;
    }

    @Override
    public String getElementType() {
        return elementType;
    }

    @Override
    public void setPrecision(int precision) {
        this.precision = precision;
    }

    @Override
    public int getPrecision() {
        return precision;
    }

    @Override
    public void setScale(int scale) {
        this.scale = scale;
    }

    @Override
    public int getScale() {
        return scale;
    }

    @Override
    public void nullable(boolean nullable) {
        this.nullable = nullable;
    }

    @Override
    public boolean isNullable() {
        return nullable;
    }

    @Override
    public void setPrimary(int primary) {
        this.primary = primary;
    }

    @Override
    public int getPrimary() {
        return primary;
    }

    @Override
    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    @Override
    public String getDefaultValue() {
        return defaultValue;
    }

}
