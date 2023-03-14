/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb.sdk.common.partition;

import java.util.List;

public interface Partition {

    String strategy();

    List<String> cols();

    List<PartitionDetailDefinition> details();
}
