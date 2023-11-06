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

package io.dingodb.sdk.common.serial;

import io.dingodb.sdk.common.serial.schema.DingoSchema;

import java.util.List;

public final class Utils {

    private Utils() {
    }

    public static void sortSchema(List<DingoSchema> schemas) {
        int flag = 1;
        for (int i = 0; i < schemas.size() - flag; i++) {
            if ((!schemas.get(i).isKey()) && lengthNotSure(schemas.get(i))) {
                int target = schemas.size() - flag++;
                while (lengthNotSure(schemas.get(target)) || schemas.get(target).isKey()) {
                    target--;
                    if (target == i) {
                        return;
                    }
                    flag++;
                }
                schemas.set(target, schemas.set(i, schemas.get(target)));
            }
        }
    }

    public static boolean lengthNotSure(DingoSchema schema) {
        return schema.getLength() == 0;
    }

    public static int[] getApproPerRecordSize(List<DingoSchema> schemas) {
        // prefix is namespace(1) and common id(8), codec version(1) and other(3)
        int keySize = 1 + 8 + 4;
        int valueSize = 4;
        for (DingoSchema schema : schemas) {
            if (schema.isKey()) {
                keySize += (schema.getLength() == 0 ? 100 : schema.getLength());
            } else {
                valueSize += (schema.getLength() == 0 ? 100 : schema.getLength());
            }
        }
        return new int[] {keySize, valueSize};
    }
}
