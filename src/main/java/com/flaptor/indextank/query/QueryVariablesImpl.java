/*
 * Copyright (c) 2011 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.flaptor.indextank.query;

import java.util.Map;
import java.util.Map.Entry;

public final class QueryVariablesImpl implements QueryVariables {
    private final Double[] vars;
    private Float lat;
    private Float lon;

    public QueryVariablesImpl(Double[] vars, Float latitude, Float longitude) {
        this.vars = vars;
        this.lat = latitude;
        this.lon = longitude;
    }

    public double getValue(int varIndex) {
        if (varIndex >= vars.length || vars[varIndex] == null)
            throw new NoSuchQueryVariableException("Query variable " + varIndex + " is not defined in this query.", varIndex);
            
        return vars[varIndex];
    }
	public int getVariablesCount() {
	    return vars.length;
	}

    public double getIpGeoLat() {
        if (lat == null)
            throw new NoSuchQueryVariableException("Query variable 'ipgeo.lat' is not defined in this query.", -1);
        return lat;
    }

    public double getIpGeoLon() {
        if (lon == null)
            throw new NoSuchQueryVariableException("Query variable 'ipgeo.lon' is not defined in this query.", -1);
        return lon;
    }

    public static QueryVariables fromMap(Map<Integer, Double> map, Float latitude, Float longitude) {
        int maxIdx = -1;
        for (Integer idx : map.keySet()) {
            maxIdx = Math.max(maxIdx, idx);
        }
        Double[] vars = new Double[maxIdx+1];
        for (Entry<Integer, Double> e : map.entrySet()) {
            vars[e.getKey()] = e.getValue();
        }
        return new QueryVariablesImpl(vars, latitude, longitude);
    }
}
