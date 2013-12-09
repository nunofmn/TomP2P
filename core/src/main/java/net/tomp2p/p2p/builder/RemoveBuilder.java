/*
 * Copyright 2012 Thomas Bocek
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

package net.tomp2p.p2p.builder;

import java.util.ArrayList;
import java.util.Collection;

import net.tomp2p.futures.FutureRemove;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;

public class RemoveBuilder extends DHTBuilder<RemoveBuilder> {
    
    private final static FutureRemove FUTURE_SHUTDOWN = new FutureRemove(null)
            .setFailed("remove builder - peer is shutting down");
    
    private Collection<Number160> contentKeys;

    private Collection<Number640> keys;

    private Number160 contentKey;

    private boolean all = false;

    private boolean returnResults = false;
    
    private Number640 from;

    private Number640 to;

    public RemoveBuilder(Peer peer, Number160 locationKey) {
        super(peer, locationKey);
        self(this);
    }

    public Collection<Number160> contentKeys() {
        return contentKeys;
    }

    public RemoveBuilder contentKeys(Collection<Number160> contentKeys) {
        this.contentKeys = contentKeys;
        return this;
    }

    public Collection<Number640> keys() {
        return keys;
    }

    public RemoveBuilder keys(Collection<Number640> keys) {
        this.keys = keys;
        return this;
    }

    public Number160 contentKey() {
        return contentKey;
    }

    public RemoveBuilder contentKey(Number160 contentKey) {
        this.contentKey = contentKey;
        return this;
    }

    public boolean isAll() {
        return all;
    }

    public RemoveBuilder setAll(boolean all) {
        this.all = all;
        return this;
    }

    public RemoveBuilder setAll() {
        this.all = true;
        return this;
    }

    public boolean isReturnResults() {
        return returnResults;
    }

    public RemoveBuilder returnResults(boolean returnResults) {
        this.returnResults = returnResults;
        return this;
    }

    public RemoveBuilder setReturnResults() {
        this.returnResults = true;
        return this;
    }
    
    public RemoveBuilder from(Number640 from) {
        this.from = from;
        return this;
    }

    public Number640 from() {
        return from;
    }

    public RemoveBuilder to(Number640 to) {
        this.to = to;
        return this;
    }

    public Number640 to() {
        return to;
    }

    public boolean isRange() {
        return from != null && to != null;
    }

    public FutureRemove start() {
        if (peer.isShutdown()) {
            return FUTURE_SHUTDOWN;
        }
        preBuild("remove-builder");
        if (all) {
            contentKeys = null;
        } else if (contentKeys == null && !all) {
            contentKeys = new ArrayList<Number160>(1);
            if (contentKey == null) {
                contentKey = Number160.ZERO;
            }
            contentKeys.add(contentKey);
        }

        return peer.getDistributedHashMap().remove(this);
    }
}
