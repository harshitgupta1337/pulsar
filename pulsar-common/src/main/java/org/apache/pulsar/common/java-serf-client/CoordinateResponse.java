/*
 * Copyright (c) 2014 Arne M. Størksen <arne.storksen@tv2.no>.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package no.tv2.serf.client;

import java.util.List;
import java.util.Map;
import org.msgpack.type.Value;

import org.apache.pulsar.common.policies.data.NetworkCoordinate;

public class CoordinateResponse extends ResponseBase {

    private final static CoordinateParser coordinateParser = new CoordinateParser();
    //private final List<Member> members;
    private final NetworkCoordinate coordinate;
    
    CoordinateResponse(Long seq, String error, Map<String, Value> response) {
        super(seq, error);
        this.coordinate = coordinateParser.parse(response);
    }

    public NetworkCoordinate getCoordinate() {
        return coordinate;
    }

}
