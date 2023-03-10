/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb.sdk.common.partition;

import java.util.List;

public interface Partition {

    String funcName();

    List<String> cols();

    List<PartitionDetail> details();
}
