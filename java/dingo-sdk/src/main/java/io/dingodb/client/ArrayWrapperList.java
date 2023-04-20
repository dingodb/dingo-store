package io.dingodb.client;

import java.util.AbstractList;

public class ArrayWrapperList<T> extends AbstractList<T> {

    private final T[] array;

    public ArrayWrapperList(T[] array) {
        this.array = array;
    }

    @Override
    public T get(int index) {
        if (index > array.length) {
            throw new ArrayIndexOutOfBoundsException();
        }
        return array[index];
    }

    @Override
    public int size() {
        return array.length;
    }
}
