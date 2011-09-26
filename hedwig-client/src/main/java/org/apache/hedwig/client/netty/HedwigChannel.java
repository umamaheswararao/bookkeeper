package org.apache.hedwig.client.netty;

import org.apache.log4j.Logger;

import org.apache.hedwig.protocol.PubSubProtocol.OperationType;
import org.apache.hedwig.util.HedwigSocketAddress;
import org.apache.hedwig.exceptions.PubSubException.CouldNotConnectException;
import org.apache.hedwig.client.data.PubSubData;
import org.apache.hedwig.client.data.TopicSubscriber;
import org.apache.hedwig.client.conf.ClientConfiguration;

import com.google.protobuf.ByteString;
import java.net.InetSocketAddress;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class HedwigChannel implements ChannelFutureListener {
    private static final Logger logger = Logger.getLogger(HedwigChannel.class);

    private enum State {
        DISCONNECTED, CONNECTING, CONNECTED;
    };
    
    private State state;
    private final InetSocketAddress host;
    private final HedwigClient client;
    private final ChannelFactory socketFactory;
    private final ClientChannelPipelineFactory pipelineFactory;
    private final Queue<PubSubData> connectionQueue;
    private final ClientConfiguration cfg;
    private Channel channel = null;

    public HedwigChannel(HedwigClient client, InetSocketAddress host) {
        this.host = host;
        this.client = client;
        this.socketFactory = client.getSocketFactory();
        this.pipelineFactory = client.getPipelineFactory();
        this.cfg = client.getConfiguration();
        connectionQueue = new ConcurrentLinkedQueue<PubSubData>();

        this.state = State.DISCONNECTED;
        logger.info("IKTODO creating channel for host " + host + " id: " + System.identityHashCode(this));//, new Exception());
    }

    /*    public void redirect(HedwigChannel redirectChannel, String topic) {
          }*/

    public boolean channelMatches(Channel other) {
        if (channel != null) {
            return channel.equals(other);
        }
        return channel == other;
    }
    
    public InetSocketAddress getHost() {
        return host;
    }

    public Channel getChannel() {
        return channel;
    }

    public void connect() { 
        logger.info("IKTODO connecting channel for host " + host + " id: " + System.identityHashCode(this));

        if (state == State.DISCONNECTED
            || state == State.CONNECTING) {
            synchronized (this) {
                if (state == State.DISCONNECTED) {
                    state = State.CONNECTING;
                } else {
                    return;
                }
            }
        } else {
            return; // already connected
        }
        logger.info("IKTODO Actually connecting" + " id: " + System.identityHashCode(this)
                + " State: " + state);
        if (logger.isDebugEnabled()) {
            logger.debug("Connecting to host: " + host);
        }
        // Set up the ClientBootStrap so we can create a new Channel connection
        // to the server.
        ClientBootstrap bootstrap = new ClientBootstrap(socketFactory);
        bootstrap.setPipelineFactory(pipelineFactory);
        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("keepAlive", true);

        // Start the connection attempt to the input server host.
        ChannelFuture future = bootstrap.connect(host);
        future.addListener(this);
    }

    public void operationComplete(ChannelFuture future) throws Exception {
        logger.info("IKTODO channel connect complete " + host
                    + " id: " + System.identityHashCode(this));

        // If the client has stopped, there is no need to proceed with any
        // callback logic here.
        if (client.hasStopped()) {
            if (future.isSuccess()) {
                logger.info("IKTODO closing channel for host, in opComplete");
                future.getChannel().close().awaitUninterruptibly();
            }
            return;
        }
        
        if (!future.isSuccess()) {
            errorMessageQueue();
            state = State.DISCONNECTED;
        } else {
            state = State.CONNECTED;
            channel = future.getChannel();
            drainMessageQueue();
        }
    }

    public void close() {
        logger.info("IKTODO closing channel for host");
        if (channel != null) {
            HedwigClient.getResponseHandlerFromChannel(channel).channelClosedExplicitly = true;
            channel.close().awaitUninterruptibly();
        }
    }

    public void disconnect() {
        errorMessageQueue();
    }

    public void sendMessage(PubSubData pubSubData) {
        logger.info("IKTODO sendMessage id: " + System.identityHashCode(this)
                    + " state = " + state + ", " + connectionQueue.size());

        connect();
        if (state == State.CONNECTED && connectionQueue.isEmpty()) {
            reallySendMessage(pubSubData);
        } else {
            synchronized (this) {
                if (state == State.CONNECTED 
                    && connectionQueue.isEmpty()) {
                    reallySendMessage(pubSubData);
                } else {
                    if (connectionQueue.isEmpty()) {
                        // IKTODO should be bounded and throw exceptions etc
                        connectionQueue.add(pubSubData);
                    } 
                }
            }
        }
    }

    synchronized private void errorMessageQueue() {
        logger.error("Error connecting to host: " + host);
        while (!connectionQueue.isEmpty()) {
            PubSubData d = connectionQueue.poll();
            // If we were not able to connect to the host, it could be down.
            ByteString hostString = ByteString.copyFromUtf8(HedwigSocketAddress.sockAddrStr(host));
            if (d.connectFailedServers != null && d.connectFailedServers.contains(hostString)) {
                // We've already tried to connect to this host before so just
                // invoke the operationFailed callback.
                logger.error("Error connecting to host more than once so just invoke the operationFailed callback!");
                d.callback.operationFailed(d.context, new CouldNotConnectException(
                                                   "Could not connect to host: " + host));
            } else {
                if (logger.isDebugEnabled())
                    logger.debug("Try to connect to server: " + host + " again for pubSubData: " + d);
                // Keep track of this current server that we failed to connect
                // to but retry the request on the default server host/VIP.
                // The topic2Host mapping might need to be updated.
                if (d.connectFailedServers == null) {
                    d.connectFailedServers = new LinkedList<ByteString>();
                }
                d.connectFailedServers.add(hostString);
                
                if (d.operationType.equals(OperationType.SUBSCRIBE)) {
                    TopicSubscriber topic = new TopicSubscriber(d.topic, d.subscriberId);
                    HedwigChannel c = client.getSubscriber().getChannel(topic, cfg.getDefaultServerHost());
                    c.sendMessage(d);
                } else {
                    HedwigChannel c = client.getChannel(cfg.getDefaultServerHost());
                    c.sendMessage(d);
                }
            }
        }
    }

    // IKTODO ensure thread safely between this and sendMessage()
    synchronized private void drainMessageQueue() {
        logger.info("IKTODO draining message queue");

        while (!connectionQueue.isEmpty()) {
            PubSubData d = connectionQueue.poll();
            reallySendMessage(d);
        } 
    }
    
    private void reallySendMessage(PubSubData d) {
        // Now that we have connected successfully to the server, see what type
        // of PubSub request this was.
        if (logger.isDebugEnabled())
            logger.debug("Connection to host: " + host + " was successful for pubSubData: " + d);
        if (d.operationType.equals(OperationType.PUBLISH)) {
            client.getPublisher().doPublish(d, channel);
        } else {
            client.getSubscriber().doSubUnsub(d, channel);
        }
    }

}