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

import io.dingodb.sdk.common.KeyValue;
import io.dingodb.sdk.common.serial.schema.DingoSchema;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class RecordDecoder {
    private final int schemaVersion;
    private List<DingoSchema> schemas;
    private final long id;

    public RecordDecoder(int schemaVersion, List<DingoSchema> schemas, long id) {
        this.schemaVersion = schemaVersion;
        this.schemas = schemas;
        this.id = id;
    }

    private void checkPrefix(Buf buf) {
        buf.skip(1);

        long keyId = buf.readLong();
        if (keyId != id) {
            throw new RuntimeException("Invalid prefix id, codec prefix id: " + id + ", key prefix id " + keyId);
        }
    }

    private int checkTag(Buf buf) {
        int codecVer = buf.reverseRead();
        if (codecVer > Config.CODEC_VERSION_V2) {
            throw new RuntimeException(
                "Invalid codec version, " + ", key codec version " + codecVer
            );
        }

        buf.reverseSkip(3);

        return codecVer;
    }

    private void checkSchemaVersion(Buf buf) {
        int schemaVer = buf.readInt();
        if (schemaVer > this.schemaVersion) {
            throw new RuntimeException(
                "Invalid schema version, schema version: " + this.schemaVersion + ", key schema version " + schemaVer
            );
        }
    }

    private int checkKeyValue(Buf keyBuf, Buf valueBuf) {
        int codecVer = checkTag(keyBuf);
        checkPrefix(keyBuf);
        checkSchemaVersion(valueBuf);

        return codecVer;
    }

    public Object[] decodeV1(KeyValue keyValue) {
        Buf keyBuf = new BufImpl(keyValue.getKey());
        Buf valueBuf = new BufImpl(keyValue.getValue());

        checkKeyValue(keyBuf, valueBuf);

        Object[] record = new Object[schemas.size()];
        for (DingoSchema<?> schema : schemas) {
            if (schema.isKey()) {
                record[schema.getIndex()] = schema.decodeKey(keyBuf);
            } else {
                if (valueBuf.isEnd()) {
                    continue;
                }
                record[schema.getIndex()] = schema.decodeValue(valueBuf);
            }
        }
        return record;
    }

    private Object[] decodeV1(KeyValue keyValue, int[] columnIndexes) {
        Buf keyBuf = new BufImpl(keyValue.getKey());
        Buf valueBuf = new BufImpl(keyValue.getValue());

        checkKeyValue(keyBuf, valueBuf);

        Object[] record = new Object[columnIndexes.length];
        int i = 0;
        for (DingoSchema<?> schema : schemas) {
            if (Arrays.binarySearch(columnIndexes, schema.getIndex()) < 0) {
                if (schema.isKey()) {
                    schema.skipKey(keyBuf);
                } else if (!valueBuf.isEnd()) {
                    schema.skipValue(valueBuf);
                }
            } else {
                if (schema.isKey()) {
                    record[i] = schema.decodeKey(keyBuf);
                } else if (!valueBuf.isEnd()) {
                    record[i] = schema.decodeValue(valueBuf);
                }
                i++;
            }
        }
        return record;
    }

    public Object[] decodeV2(KeyValue keyValue) {
        Buf keyBuf = new BufImpl(keyValue.getKey());
        Buf valueBuf = new BufImpl(keyValue.getValue());

        checkKeyValue(keyBuf, valueBuf);

        //decode value.
        int cntNotNullCols = valueBuf.readShort();
        int cntNullCols = valueBuf.readShort();
        int totalColCnt = cntNotNullCols + cntNullCols;
        int idsPos = 4 + 2 * 2;
        int offsetPos = idsPos + 2 * totalColCnt;
        int dataPos = offsetPos + 4 * totalColCnt;

        //get id-offset map.
        Map<Short, Integer> idOffsetMap = new TreeMap<Short, Integer>();

        for (int i = 0; i < totalColCnt; i++) {
            short id = valueBuf.readShortAt(idsPos);
            int offset = valueBuf.readIntAt(offsetPos);
            idOffsetMap.put(id, offset);
            idsPos += 2;
            offsetPos += 4;
        }

        valueBuf.setForwardOffset(dataPos);

        Object[] record = new Object[schemas.size()];
        for (DingoSchema<?> schema : schemas) {
            if (schema.isKey()) {
                record[schema.getIndex()] = schema.decodeKeyV2(keyBuf);
            } else {
                if (valueBuf.isEnd()) {
                    continue;
                }

                int index = schema.getIndex();
                if (idOffsetMap.get((short)index) == -1) {
                    //null column.
                    record[index] = null;
                } else {
                    record[index] = schema.decodeValueV2(valueBuf);
                }
            }
        }
        return record;
    }

    private Object[] decodeV2(KeyValue keyValue, int[] columnIndexes) {
        Buf keyBuf = new BufImpl(keyValue.getKey());
        Buf valueBuf = new BufImpl(keyValue.getValue());

        checkKeyValue(keyBuf, valueBuf);

        //decode value.
        int cntNotNullCols = valueBuf.readShort();
        int cntNullCols = valueBuf.readShort();
        int totalColCnt = cntNotNullCols + cntNullCols;
        int idsPos = 4 + 2 * 2;
        int offsetPos = idsPos + 2 * totalColCnt;
        int dataPos = offsetPos + 4 * totalColCnt;

        //get id-offset map.
        Map<Short, Integer> idOffsetMap = new TreeMap<Short, Integer>();

        for (int i = 0; i < totalColCnt; i++) {
            short id = valueBuf.readShortAt(idsPos);
            int offset = valueBuf.readIntAt(offsetPos);
            idOffsetMap.put(id, offset);
            idsPos += 2;
            offsetPos += 4;
        }

        valueBuf.setForwardOffset(dataPos);

        Object[] record = new Object[columnIndexes.length];
        int i = 0;
        for (DingoSchema<?> schema : schemas) {
            if (Arrays.binarySearch(columnIndexes, schema.getIndex()) < 0) {
                if (schema.isKey()) {
                    schema.skipKeyV2(keyBuf);
                } else if (!valueBuf.isEnd()) {
                    if (idOffsetMap.get((short)schema.getIndex()) != -1) { //null
                        schema.skipValueV2(valueBuf);
                    }
                }
            } else {
                if (schema.isKey()) {
                    record[i] = schema.decodeKeyV2(keyBuf);
                } else if (!valueBuf.isEnd()) {
                    if (idOffsetMap.get((short)schema.getIndex()) == -1) { //null
                        record[i] = null;
                    } else {
                        record[i] = schema.decodeValueV2(valueBuf);
                    }
                }
                i++;
            }
        }
        return record;
    }

    public Object[] decode(KeyValue keyValue) {
        Buf keyBuf = new BufImpl(keyValue.getKey());
        Buf valueBuf = new BufImpl(keyValue.getValue());

        int codecVer = checkKeyValue(keyBuf, valueBuf);
        if (codecVer <= Config.CODEC_VERSION) {
            return decodeV1(keyValue);
        } else {
            return decodeV2(keyValue);
        }
    }


    public Object[] decode(KeyValue keyValue, int[] columnIndexes) {
        Buf keyBuf = new BufImpl(keyValue.getKey());
        Buf valueBuf = new BufImpl(keyValue.getValue());

        int codecVer = checkKeyValue(keyBuf, valueBuf);
        if (codecVer <= Config.CODEC_VERSION) {
            return decodeV1(keyValue, columnIndexes);
        } else {
            return decodeV2(keyValue, columnIndexes);
        }
    }

    public Object[] decodeKeyPrefix(byte[] keyPrefix) {
        Buf keyPrefixBuf = new BufImpl(keyPrefix);

        keyPrefixBuf.skip(1);

        if (keyPrefixBuf.readLong() != id) {
            throw new RuntimeException("Wrong Common Id");
        }
        Object[] record = new Object[schemas.size()];
        for (DingoSchema<?> schema : schemas) {
            if (keyPrefixBuf.isEnd()) {
                break;
            }
            if (schema.isKey()) {
                record[schema.getIndex()] = schema.decodeKeyPrefix(keyPrefixBuf);
            }
        }
        return record;
    }

    private Object[] decodeValueV1(KeyValue keyValue, int[] columnIndexes) {
        Buf valueBuf = new BufImpl(keyValue.getValue());
        if (valueBuf.readInt() > schemaVersion) {
            throw new RuntimeException("Wrong Schema Version");
        }
        Object[] record = new Object[schemas.size()];
        for (DingoSchema<?> schema : schemas) {
            if (valueBuf.isEnd()) {
                break;
            }
            if (!schema.isKey()) {
                if (Arrays.binarySearch(columnIndexes, schema.getIndex()) < 0) {
                    schema.skipValue(valueBuf);
                } else {
                    record[schema.getIndex()] = schema.decodeValue(valueBuf);
                }
            }
        }
        return record;
    }

    private Object[] decodeValueV2(KeyValue keyValue, int[] columnIndexes) {
        Buf valueBuf = new BufImpl(keyValue.getValue());
        if (valueBuf.readInt() > schemaVersion) {
            throw new RuntimeException("Wrong Schema Version");
        }

        //decode value.
        int cntNotNullCols = valueBuf.readShort();
        int cntNullCols = valueBuf.readShort();
        int totalColCnt = cntNotNullCols + cntNullCols;
        int idsPos = 4 + 2 * 2;
        int offsetPos = idsPos + 2 * totalColCnt;
        int dataPos = offsetPos + 4 * totalColCnt;

        //get id-offset map.
        Map<Short, Integer> idOffsetMap = new HashMap<Short, Integer>();

        for (int i = 0; i < totalColCnt; i++) {
            short id = valueBuf.readShortAt(idsPos);
            int offset = valueBuf.readIntAt(offsetPos);
            idOffsetMap.put(id, offset);
            idsPos += 2;
            offsetPos += 4;
        }

        valueBuf.setForwardOffset(dataPos);

        Object[] record = new Object[schemas.size()];
        for (DingoSchema<?> schema : schemas) {
            if (valueBuf.isEnd()) {
                break;
            }
            if (!schema.isKey()) {
                if (Arrays.binarySearch(columnIndexes, schema.getIndex()) < 0) {
                    if (idOffsetMap.get((short)schema.getIndex()) != -1) {
                        schema.skipValueV2(valueBuf);
                    }
                } else {
                    record[schema.getIndex()] = schema.decodeValueV2(valueBuf);
                }
            }
        }
        return record;
    }

    public Object[] decodeValue(KeyValue keyValue, int[] columnIndexes) {
        Buf keyBuf = new BufImpl(keyValue.getKey());
        Buf valueBuf = new BufImpl(keyValue.getValue());

        int codecVer = checkKeyValue(keyBuf, valueBuf);

        if (codecVer == Config.CODEC_VERSION) {
            return decodeValueV1(keyValue, columnIndexes);
        } else {
            return decodeValueV2(keyValue, columnIndexes);
        }
    }
}
