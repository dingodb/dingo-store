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

package io.dingodb.sdk.common;

public interface DingoCommonId {

    Type type();

    long parentId();

    long entityId();

    enum Type {
        ENTITY_TYPE_SCHEMA,
        ENTITY_TYPE_TABLE,
        ENTITY_TYPE_PART,
        ENTITY_TYPE_INDEX,
        ENTITY_TYPE_REGION;
    }

    static boolean equals(DingoCommonId id1, DingoCommonId id2) {
        if (id1 == id2) {
            return true;
        }

        if (id1.parentId() != id2.parentId()) {
            return false;
        }
        if (id1.entityId() != id2.entityId()) {
            return false;
        }
        return id1.type() == id2.type();
    }

    static int hashCode(DingoCommonId id) {
        int result = id.type().hashCode();
        result = 31 * result + (int) (id.parentId() ^ (id.parentId() >>> 32));
        result = 31 * result + (int) (id.entityId() ^ (id.entityId() >>> 32));
        return result;
    }

}
