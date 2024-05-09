#/usr/bin/env python3

from os.path import dirname, abspath
import argparse

import dingosdk

dir = dirname(dirname(dirname(abspath(__file__))))

parser = argparse.ArgumentParser(description='argparse')
parser.add_argument('--coordinator_url', '-url', type=str, default ="file://"+ dir+"/bin/coor_list", help="coordinator url, try to use like file://./coor_list")
args = parser.parse_args()

g_region_ids=[]

s, g_client = dingosdk.Client.Build(args.coordinator_url)
assert s.ok(), "client build fail"


def create_region(name: str, start_key: str, end_key: str, replicas: int = 3,
                  engine_type = dingosdk.kLSM):
    assert name, "name should not be empty"
    assert start_key, "start_key should not be empty"
    assert end_key, "end_key should not be empty"
    assert start_key < end_key, "start_key must be less than end_key"
    assert replicas > 0, "replicas must be greater than 0"

    s, region_creator = g_client.NewRegionCreator()
    assert s.ok(), "dingo region creator build fail"

    region_creator.SetRegionName(name)
    region_creator.SetRange(start_key, end_key)
    region_creator.SetEngineType(engine_type)
    region_creator.SetReplicaNum(replicas)
    region_creator.Wait(True)
    s, region_id = region_creator.Create()

    print(f"Create region status: {s.ToString()}, region_id: {region_id}")

    if s.ok() :
        assert region_id > 0
        s, inprogress = g_client.IsCreateRegionInProgress(region_id)
        assert not inprogress
        g_region_ids.append(region_id)


def post_clean():
    for region_id in g_region_ids:
        s = g_client.DropRegion(region_id)
        print(f"Drop region status: {s.ToString()}, region_id: {region_id}")
        s, inprogress = g_client.IsCreateRegionInProgress(region_id)
        print(f"Query region status: {s.ToString()}, region_id: {region_id}, {inprogress}")
    g_region_ids.clear()


def raw_kv_example():
    built, raw_kv = g_client.NewRawKV()
    assert built.ok(), "dingo raw_kv build fail"

    # put/get/delete
    key = "wb01"
    value = "pong"
    put = raw_kv.Put(key, value)
    print(f"raw_kv put status:{put.ToString()}, key: {key}, value: {value}")

    got, to_get= raw_kv.Get(key)
    print(f"raw_kv get status:{got.ToString()}, key: {key}, value: {to_get}")

    dele = raw_kv.Delete(key)
    print(f"raw_kv delete status:{dele.ToString()}, key: {key}")
    if dele.ok():
        got, tmp= raw_kv.Get(key)
        print(f"raw_kv get status:{got.ToString()}, key: {key}, value: {tmp}")

    keys = ["wb01", "wc01", "wd01", "wf01", "wl01", "wm01"]
    values = ["rwb01", "rwc01", "rwd01", "rwf01", "rl01", "rm01"]

    # batch put/batch get/batch delete
    kvs = dingosdk.KVPairVector()
    for i in range(len(keys)):
        kv = dingosdk.KVPair()
        kv.key = keys[i]
        kv.value = values[i]
        kvs.push_back(kv)

    result = raw_kv.BatchPut(kvs)
    print(f"raw_kv batch_put: {result.ToString()}")

    batch_get_values = dingosdk.KVPairVector()
    result = raw_kv.BatchGet(keys, batch_get_values)
    print(f"raw_kv batch_get: {result.ToString()}")
    if result.ok():
        for kv in batch_get_values:
            print(f"raw_kv batch_get key: {kv.key}, value: {kv.value}")

    result = raw_kv.BatchDelete(keys)
    print(f"raw_kv batch_delete: {result.ToString()}")

    tmp_batch_get_values = dingosdk.KVPairVector()
    result = raw_kv.BatchGet(keys, tmp_batch_get_values)
    print(f"raw_kv batch_get after batch delete: {result.ToString()}")
    if result.ok():
        for kv in tmp_batch_get_values:
            print(f"raw_kv batch_get after delete, key: {kv.key}, value: {kv.value}")

    assert len(tmp_batch_get_values) == 0

    # put if absent
    key = "wb01"
    value = "pong"
    result, state = raw_kv.PutIfAbsent(key, value)
    print(f"raw_kv put_if_absent: {result.ToString()}, state: {state}")

    result, to_get= raw_kv.Get(key)
    print(f"raw_kv get after put_if_absent: {result.ToString()}, value: {to_get}")
    if result.ok():
        assert value == to_get

    result, again_state= raw_kv.PutIfAbsent(key, value)
    print(f"raw_kv put_if_absent again: {result.ToString()}, state: {again_state}")

    result = raw_kv.Delete(key)
    print(f"raw_kv delete: {result.ToString()}")
    if result.ok():
        result, tmp= raw_kv.Get(key)
        print(f"raw_kv get after delete: {result.ToString()}, value: {tmp}")

    # batch put if absent
    keys_state = dingosdk.KeyOpStateVector()
    result = raw_kv.BatchPutIfAbsent(kvs, keys_state)
    print(f"raw_kv batch_put_if_absent: {result.ToString()}")
    if result.ok():
        for key_state in keys_state:
            print(f"raw_kv batch_put_if_absent, key: {key_state.key}, state: {key_state.state}")

    batch_get_values = dingosdk.KVPairVector()
    result = raw_kv.BatchGet(keys, batch_get_values)
    print(f"raw_kv batch_get after batch_put_if_absent: {result.ToString()}")
    if result.ok():
        for kv in batch_get_values:
            print(f"raw_kv batch_get after batch_put_if_absent, key: {kv.key}, value: {kv.value}")

    again_keys_state = dingosdk.KeyOpStateVector()
    result = raw_kv.BatchPutIfAbsent(kvs, again_keys_state)
    print(f"raw_kv batch_put_if_absent again: {result.ToString()}")
    if result.ok():
        for key_state in again_keys_state:
            print(f"raw_kv batch_put_if_absent again, key: {key_state.key}, state: {key_state.state}")

    result = raw_kv.BatchDelete(keys)
    print(f"raw_kv batch_delete: {result.ToString()}")

    tmp_batch_get_values = dingosdk.KVPairVector()
    result = raw_kv.BatchGet(keys, tmp_batch_get_values)
    print(f"raw_kv batch_get after batch delete: {result.ToString()}")
    if result.ok():
        for kv in tmp_batch_get_values:
            print(f"raw_kv batch_get after delete, key: {kv.key}, value: {kv.value}")
        assert len(tmp_batch_get_values) == 0

    # delete range
    result = raw_kv.BatchPut(kvs)
    print(f"raw_kv batch_put: {result.ToString()}")

    batch_get_values = dingosdk.KVPairVector()
    result = raw_kv.BatchGet(keys, batch_get_values)
    print(f"raw_kv batch_get: {result.ToString()}")
    if result.ok():
        for kv in batch_get_values:
            print(f"raw_kv batch_get key: {kv.key}, value: {kv.value}")

    result, delete_count = raw_kv.DeleteRange("wb01", "wz01")
    print(f"raw_kv delete range: {result.ToString()}, delete_count: {delete_count}")

    result, delete_count = raw_kv.DeleteRangeNonContinuous("wb01", "wz01")
    print(f"raw_kv delete range non continuous: {result.ToString()}, delete_count: {delete_count}")

    tmp_batch_get_values = dingosdk.KVPairVector()
    result = raw_kv.BatchGet(keys, tmp_batch_get_values)
    print(f"raw_kv batch_get after delete_range: {result.ToString()}")
    if result.ok():
        for kv in tmp_batch_get_values:
            print(f"raw_kv batch_get after delete_range, key: {kv.key}, value: {kv.value}")

    # compare and set
    key = "wb01"
    value = "pong"

    result, state= raw_kv.CompareAndSet(key, value, "")
    print(f"raw_kv compare_and_set: {result.ToString()}, key: {key}, value: {value}, expect: empty, state: {state}")

    result, to_get = raw_kv.Get(key)
    print(f"raw_kv get after compare_and_set: {result.ToString()}, value: {to_get}")
    if result.ok():
        assert value == to_get

    result, again_state= raw_kv.CompareAndSet(key, "ping", value)
    print(f"raw_kv compare_and_set again: {result.ToString()}, key: {key}, value: ping, expect: {value}, state: {again_state}")

    result, again_get= raw_kv.Get(key)
    print(f"raw_kv get after compare_and_set again: {result.ToString()}, value: {again_get}")
    if result.ok():
        assert "ping" == again_get

    result = raw_kv.Delete(key)
    print(f"raw_kv delete: {result.ToString()}")
    if result.ok():
        result, tmp= raw_kv.Get(key)
        print(f"raw_kv get after delete: {result.ToString()}, value: {tmp}")
        assert tmp == ""

    # batch compare and set
    kvs = dingosdk.KVPairVector()
    for i in range(len(keys)):
        kv = dingosdk.KVPair()
        kv.key = keys[i]
        kv.value = values[i]
        kvs.push_back(kv)

    expect_values = [""] * len(kvs)

    keys_state = dingosdk.KeyOpStateVector()
    result = raw_kv.BatchCompareAndSet(kvs, expect_values, keys_state)
    print(f"raw_kv batch_compare_and_set: {result.ToString()}")
    if result.ok():
        for key_state in keys_state:
            print(f"raw_kv batch_compare_and_set, key: {key_state.key}, state: {key_state.state}")
            assert key_state.state

    batch_get_values = dingosdk.KVPairVector()
    result = raw_kv.BatchGet(keys, batch_get_values)
    print(f"raw_kv batch_get after batch_compare_and_set: {result.ToString()}")
    if result.ok():
        for kv in batch_get_values:
            print(f"raw_kv batch_get after batch_compare_and_set, key: {kv.key}, value: {kv.value}")
            find = False
            for ele in kvs:
                if ele.key == kv.key:
                    assert ele.key == kv.key
                    assert ele.value == kv.value
                    find = True
            assert find

    # batch compare and set again
    kvs = dingosdk.KVPairVector()
    for key in keys:
        kv = dingosdk.KVPair()
        kv.key = key
        kv.value = "ping"
        kvs.push_back(kv)

    expect_values = values.copy()

    assert len(kvs) == len(expect_values)

    again_keys_state = dingosdk.KeyOpStateVector()
    result = raw_kv.BatchCompareAndSet(kvs, expect_values, again_keys_state)
    print(f"raw_kv batch_compare_and_set again: {result.ToString()}")
    if result.ok():
        for key_state in again_keys_state:
            print(f"raw_kv batch_compare_and_set again, key: {key_state.key}, state: {key_state.state}")
            assert key_state.state

    batch_get_values = dingosdk.KVPairVector()
    result = raw_kv.BatchGet(keys, batch_get_values)
    print(f"raw_kv batch_get after batch_compare_and_set again: {result.ToString()}")
    if result.ok():
        for kv in batch_get_values:
            print(f"raw_kv batch_get after batch_compare_and_set again, key: {kv.key}, value: {kv.value}")
            find = False
            for ele in kvs:
                if ele.key == kv.key:
                    assert ele.key == kv.key
                    assert ele.value == kv.value
                    find = True
            assert find

    result = raw_kv.BatchDelete(keys)
    print(f"raw_kv batch_delete: {result.ToString()}")

    tmp_batch_get_values = dingosdk.KVPairVector()
    result = raw_kv.BatchGet(keys, tmp_batch_get_values)
    print(f"raw_kv batch_get after batch delete: {result.ToString()}")
    if result.ok():
        for kv in tmp_batch_get_values:
            print(f"raw_kv batch_get after delete, key: {kv.key}, value: {kv.value}")
        assert len(tmp_batch_get_values) == 0

    # scan
    kvs = dingosdk.KVPairVector()
    for i in range(len(keys)):
        kv = dingosdk.KVPair()
        kv.key = keys[i]
        kv.value = values[i]
        kvs.push_back(kv)

    result = raw_kv.BatchPut(kvs)
    print(f"raw_kv batch_put before scan: {result.ToString()}")

    batch_get_values = dingosdk.KVPairVector()
    result = raw_kv.BatchGet(keys, batch_get_values)
    print(f"raw_kv batch_get before scan: {result.ToString()}")
    if result.ok():
        for kv in batch_get_values:
            print(f"raw_kv batch_get before scan key: {kv.key}, value: {kv.value}")

    scan_values = dingosdk.KVPairVector()
    result = raw_kv.Scan("wa00000000", "wz00000000", 0, scan_values)
    print(f"raw_kv scan: {result.ToString()}")
    if result.ok():
        for kv in scan_values:
            print(f"raw_kv scan key: {kv.key}, value: {kv.value}")


if __name__ == "__main__":
    create_region("skd_example01", "wa00000000", "wc00000000", 3)
    create_region("skd_example02", "wc00000000", "we00000000", 3)
    create_region("skd_example03", "we00000000", "wg00000000", 3)
    create_region("skd_example04", "wl00000000", "wn00000000", 3)
    raw_kv_example()
    post_clean()

    create_region("skd_example01", "wa00000000", "wc00000000", 3, dingosdk.kBTree)
    create_region("skd_example02", "wc00000000", "we00000000", 3, dingosdk.kBTree)
    create_region("skd_example03", "we00000000", "wg00000000", 3, dingosdk.kBTree)
    create_region("skd_example04", "wl00000000", "wn00000000", 3, dingosdk.kBTree)
    raw_kv_example()
    post_clean()