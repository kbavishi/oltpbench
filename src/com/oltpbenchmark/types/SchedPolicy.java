/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

/**
 * 
 */
package com.oltpbenchmark.types;

import java.util.HashMap;
import java.util.Map;

public enum SchedPolicy {
    FIFO(1),
    EDF(2),
    EDF_PRED_LOC(4),
    EDF_PRED_LOC_OLD(3),
    EDF_PRED_BUF_LOC(5),
    GEDF(6),
    GEDF_PRED_LOC(7),
    GEDF_PRED_LOC_OLD(8),
    GEDF_PRED_BUF_LOC(9);

    private final int policy;
    private static Map map = new HashMap<>();

    SchedPolicy(int policy) {
	    this.policy = policy;
    }

    public int getPolicyAsInt() {
        return policy;
    }

    public String getPolicyAsString() {
        return String.valueOf(policy);
    }

    static {
        for (SchedPolicy policy : SchedPolicy.values()) {
            map.put(policy.policy, policy);
        }
    }

    public static SchedPolicy convertFromString(String inputPolicy) {
        for (SchedPolicy policy : SchedPolicy.values()) {
            if (policy.name().equals(inputPolicy)) {
                return policy;
            }
        }
        return null;
    }

    public static SchedPolicy valueOf(int inputPolicy) {
        return (SchedPolicy) map.get(inputPolicy);
    }
}
