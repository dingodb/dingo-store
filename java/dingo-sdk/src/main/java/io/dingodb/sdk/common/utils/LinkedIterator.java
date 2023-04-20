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

package io.dingodb.sdk.common.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

public class LinkedIterator<T> implements java.util.Iterator<T> {

    private final List<java.util.Iterator<T>> iterators = new ArrayList<>();
    private java.util.Iterator<T> currentIterator;
    private int position = 0;
    private boolean hasNext = true;

    public synchronized LinkedIterator<T> append(java.util.Iterator<T> iterator) {
        Optional.ifPresent(iterator, iterators::add);
        return this;
    }

    @Override
    public boolean hasNext() {
        if (!hasNext) {
            return false;
        }
        if (currentIterator == null || !currentIterator.hasNext()) {
            if (position < iterators.size()) {
                currentIterator = iterators.get(position++);
                return hasNext();
            } else {
                return hasNext = false;
            }
        }
        return true;
    }

    @Override
    public T next() {
        if (hasNext()) {
            return currentIterator.next();
        }
        throw new NoSuchElementException();
    }
}
