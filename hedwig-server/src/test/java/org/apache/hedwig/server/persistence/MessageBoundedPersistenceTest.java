/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hedwig.server.persistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.hedwig.client.HedwigClient;
import org.apache.hedwig.client.api.MessageHandler;
import com.google.protobuf.ByteString;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;

import org.apache.hedwig.client.api.Client;
import org.apache.hedwig.client.api.Subscriber;
import org.apache.hedwig.client.api.Publisher;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.util.Callback;

import org.apache.hedwig.server.HedwigHubTestBase;
import org.apache.hedwig.server.common.ServerConfiguration;

public class MessageBoundedPersistenceTest extends HedwigHubTestBase {
    protected static Logger logger = LoggerFactory.getLogger(MessageBoundedPersistenceTest.class);

    protected class SmallReadAheadServerConfiguration 
        extends HedwigHubTestBase.HubServerConfiguration {
        SmallReadAheadServerConfiguration(int serverPort, int sslServerPort) {
            super(serverPort, sslServerPort);
        }
        public long getMaximumCacheSize() {
            return 1;
        }

        public int getReadAheadCount() {
            return 1; 
        }
    }

    protected ServerConfiguration getServerConfiguration(int serverPort, int sslServerPort) {
        return new SmallReadAheadServerConfiguration(serverPort, sslServerPort);
    }

    @Test
    public void testBasicBounding() throws Exception {
        Client client = new HedwigClient(new ClientConfiguration());
        Publisher pub = client.getPublisher();
        Subscriber sub = client.getSubscriber();
        
        ByteString topic = ByteString.copyFromUtf8("basicBoundingTopic");
        ByteString subid = ByteString.copyFromUtf8("basicBoundingSubId");
        sub.subscribe(topic, subid, CreateOrAttach.CREATE_OR_ATTACH, 5);
        sub.closeSubscription(topic, subid);

        for (int i = 0; i < 100; i++) {
            pub.publish(topic, Message.newBuilder().setBody(ByteString.copyFromUtf8("Message #" + i)).build());
        }

        sub.subscribe(topic, subid, CreateOrAttach.ATTACH);
        sub.startDelivery(topic, subid, new MessageHandler () {
                public void deliver(ByteString topic, ByteString subscriberId, 
                                    Message msg, Callback<Void> callback,
                                    Object context) {
                    logger.info("Received " + msg.getBody().toStringUtf8());
                    callback.operationFinished(context, null);
                }
            });

        Thread.sleep(100000);
    }
}
