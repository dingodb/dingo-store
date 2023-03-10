/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb.sdk.common.partition;

import java.util.List;

public interface PartitionDetail {

    String getPartName();

    String getOperator();

    List<Object> getOperand();
}
