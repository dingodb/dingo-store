package io.dingodb.sdk.common.table;

import io.dingodb.meta.Meta;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Delegate;

import java.util.Objects;

import static io.dingodb.sdk.common.utils.Parameters.cleanNull;

@ToString(onlyExplicitlyIncluded = true)
public class ProtoColumnDefinition implements Column {

    @Getter
    @Delegate
    @ToString.Include
    private final Meta.ColumnDefinition delegate;

    private
    ProtoColumnDefinition(Meta.ColumnDefinition delegate) {
        this.delegate = delegate;
    }

    @Override
    public String getName() {
        return this.delegate.getName().toUpperCase();
    }

    @Override
    public String getType() {
        return this.delegate.getSqlType();
    }

    @Override
    public boolean isNullable() {
        return this.delegate.getNullable();
    }

    @Override
    public int getPrimary() {
        return this.delegate.getIndexOfKey();
    }

    @Override
    public String getDefaultValue() {
        return this.delegate.getDefaultVal();
    }

    @Override
    public boolean isAutoIncrement() {
        return this.delegate.getIsAutoIncrement();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof Column) {
            return copy((Column) o).delegate.equals(this.delegate);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(delegate);
    }

    public static ProtoColumnDefinition from(Meta.ColumnDefinition from) {
        return new ProtoColumnDefinition(from);
    }

    public static ProtoColumnDefinition copy(Column column) {
        return new ProtoColumnDefinition(
            Meta.ColumnDefinition.newBuilder()
                .setName(column.getName())
                .setNullable(column.isNullable())
                .setElementType(cleanNull(column.getElementType(), ""))
                .setDefaultVal(cleanNull(column.getDefaultValue(), ""))
                .setPrecision(column.getPrecision())
                .setScale(column.getScale())
                .setIndexOfKey(column.getPrimary())
                .setSqlType(column.getType())
                .setIsAutoIncrement(column.isAutoIncrement())
                .build()
        );
    }

}
