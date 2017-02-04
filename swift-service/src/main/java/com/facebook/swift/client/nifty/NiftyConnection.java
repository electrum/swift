/*
 * Copyright (C) 2013 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.facebook.swift.client.nifty;

import com.facebook.nifty.client.RequestChannel;
import com.facebook.nifty.core.RequestContext;
import com.facebook.nifty.core.RequestContexts;
import com.facebook.nifty.duplex.TDuplexProtocolFactory;
import com.facebook.swift.client.MethodEventHandler;
import com.facebook.swift.transport.ClientEventHandler;
import com.facebook.swift.transport.ConnectionContext;
import com.facebook.swift.transport.MethodMetadata;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.jboss.netty.buffer.ChannelBuffer;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class NiftyConnection
        implements Closeable
{
    private final AtomicInteger sequenceId = new AtomicInteger(42);

    private final ConnectionContext connectionContext;
    private final RequestChannel channel;
    private final CloseHandler closeHandler;

    public NiftyConnection(RequestChannel channel, ConnectionContext connectionContext)
    {
        this.channel = requireNonNull(channel, "channel is null");
        this.connectionContext = requireNonNull(connectionContext, "connectionContext is null");
        this.closeHandler = new CloseHandler(channel::close);
    }

    public TDuplexProtocolFactory getProtocolFactory()
    {
        return channel.getProtocolFactory();
    }

    public ListenableFuture<Object> invoke(NiftyProtocol protocol, List<ClientEventHandler<?>> handlers, MethodMetadata method, List<Object> parameters)
            throws Exception
    {
        if (channel.hasError()) {
            // signal that the request should be retried
            throw new TTransportException(TTransportException.NOT_OPEN, channel.getError());
        }

        MethodEventHandler eventHandler = new MethodEventHandler(handlers, method.getName(), connectionContext);
        int sequenceId = this.sequenceId.incrementAndGet();

        SettableFuture<Object> future = SettableFuture.create();
        RequestContext requestContext = RequestContexts.getCurrentContext();

        // write request
        eventHandler.preWrite(parameters);
        ChannelBuffer requestBuffer = protocol.writeRequest(sequenceId, method, parameters);
        eventHandler.postWrite(parameters);

        // send message and setup listener to handle the response
        CloseHandler.RequestTracker requestTracker = closeHandler.beginRequest();
        try {
            channel.sendAsynchronousRequest(requestBuffer, method.isOneway(), new RequestChannel.Listener()
            {
                @Override
                public void onRequestSent()
                {
                    if (method.isOneway()) {
                        requestTracker.close();
                        eventHandler.done();
                        try {
                            future.set(null);
                        }
                        catch (Exception e) {
                            future.setException(e);
                        }
                    }
                }

                @Override
                public void onResponseReceived(ChannelBuffer responseBuffer)
                {
                    requestTracker.close();

                    RequestContext oldRequestContext = RequestContexts.getCurrentContext();
                    RequestContexts.setCurrentContext(requestContext);

                    eventHandler.preRead();
                    try {
                        Object results = protocol.readResponse(responseBuffer, sequenceId, method);
                        eventHandler.postRead(results);

                        eventHandler.done();
                        future.set(results);
                    }
                    catch (Throwable e) {
                        if (e instanceof InterruptedException) {
                            Thread.currentThread().interrupt();
                            e = new TException(e);
                        }
                        eventHandler.postReadException(e);

                        eventHandler.done();
                        future.setException(e);
                    }
                    finally {
                        RequestContexts.setCurrentContext(oldRequestContext);
                    }
                }

                @Override
                public void onChannelError(TException e)
                {
                    requestTracker.close();

                    RequestContext oldRequestContext = RequestContexts.getCurrentContext();
                    RequestContexts.setCurrentContext(requestContext);
                    try {
                        eventHandler.postReadException(e);

                        eventHandler.done();
                        future.setException(e);
                    }
                    finally {
                        RequestContexts.setCurrentContext(oldRequestContext);
                    }
                }
            });
        }
        catch (Throwable throwable) {
            requestTracker.close();
            throw throwable;
        }

        return future;
    }

    @Override
    public void close()
    {
        closeHandler.close();
    }

    private static final class CloseHandler
    {
        private enum State
        {
            RUNNING, CLOSING, CLOSED
        }

        private final Runnable onClosed;

        @GuardedBy("this")
        private int pendingRequests;

        @GuardedBy("this")
        private State state = State.RUNNING;

        public CloseHandler(Runnable onClosed)
        {
            this.onClosed = onClosed;
        }

        public synchronized RequestTracker beginRequest()
                throws TTransportException
        {
            if (state != State.RUNNING) {
                throw new TTransportException(TTransportException.NOT_OPEN, "Connection is closed");
            }
            pendingRequests++;
            return new RequestTracker();
        }

        private void endRequest()
        {
            boolean runClosedHandler = false;
            synchronized (this) {
                pendingRequests--;
                checkState(pendingRequests >= 0);

                // if closing and we released the last permit, run the close handler
                if (state == State.CLOSING && pendingRequests == 0) {
                    runClosedHandler = true;
                    state = State.CLOSED;
                }
            }
            if (runClosedHandler) {
                onClosed.run();
            }
        }

        public void close()
        {
            boolean runClosedHandler = false;
            synchronized (this) {
                if (state == State.CLOSED) {
                    return;
                }

                state = State.CLOSING;

                // if no more pending request, run the close handler
                if (pendingRequests == 0) {
                    runClosedHandler = true;
                    state = State.CLOSED;
                }
            }
            if (runClosedHandler) {
                onClosed.run();
            }
        }

        public class RequestTracker
                implements Closeable
        {
            private AtomicBoolean done = new AtomicBoolean();

            public void close()
            {
                if (done.compareAndSet(false, true)) {
                    endRequest();
                }
            }
        }
    }
}
