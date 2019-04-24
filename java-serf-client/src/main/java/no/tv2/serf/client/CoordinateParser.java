/*
 * Copyright (c) 2014 Arne M. Størksen <arne.storksen@tv2.no>.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package no.tv2.serf.client;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import static no.tv2.serf.client.ResponseBase.valueConverter;
import org.msgpack.type.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.pulsar.common.policies.data.NetworkCoordinate;

class CoordinateParser {
   
    public static final Logger log = LoggerFactory.getLogger(CoordinateParser.class);

    public NetworkCoordinate parse(Map<String, Value> body) {
       
        double[] coordinateVector = new double[]{1,1,1,1,1,1,1,1}; 
        NetworkCoordinate coordinate = new NetworkCoordinate(1,1,1,coordinateVector);

        Map<String, Value> map = (Map<String, Value>) body;

        if(map.containsKey("Ok")) {
            if(map.get("Ok").toString().equals("true"))
            {
                if(map.containsKey("Coord")) {
                    coordinate = parseCoordinate(map.get("Coord"));
                }
            }
            else 
            {
                log.warn("Response not valid!");
            }
        }
        
        
        
        return coordinate;
    }

    private NetworkCoordinate parseCoordinate(Value v) {

        double adjustment = 0.0;
        double error = 0.0;
        double height = 0.0;
        double[] coordinateVector = new double[8];

        for (Map.Entry<Value, Value> entry1 : v.asMapValue().entrySet()) {
            String key = entry1.getKey().asRawValue().getString();
            Value value = entry1.getValue();
            switch(key) {
                case "Adjustment": adjustment = valueConverter().asDouble(value); break;
                case "Error": error = valueConverter().asDouble(value); break;
                case "Height": height = valueConverter().asDouble(value); break;
                case "Vec":
                        Value[] valArray = value.asArrayValue().getElementArray();
                        for(int i = 0; i < valArray.length; i++) {
                            coordinateVector[i] = valueConverter().asDouble(valArray[i]);
                        }
                        break;
            }
        }
        return new NetworkCoordinate(adjustment, error, height, coordinateVector);
    }

}
