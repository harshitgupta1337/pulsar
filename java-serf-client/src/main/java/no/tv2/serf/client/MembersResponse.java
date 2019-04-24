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

public class MembersResponse extends ResponseBase {

    private final static MembersParser membersParser = new MembersParser();
    private final List<Member> members;
    
    MembersResponse(Long seq, String error, Map<String, Value> response) {
        super(seq, error);
        this.members = membersParser.parse(response);
    }

    public List<Member> getMembers() {
        return members;
    }

}
