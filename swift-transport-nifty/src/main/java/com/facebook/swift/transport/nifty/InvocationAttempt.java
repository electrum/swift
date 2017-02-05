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
package com.facebook.swift.transport.nifty;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.apache.thrift.transport.TTransportException.NOT_OPEN;

class InvocationAttempt
{
    private final Iterator<HostAndPort> addresses;
    private final NiftyConnectionManager niftyConnectionManager;
    private final InvocationFunction<NiftyConnection> invocationFunction;
    private final Consumer<HostAndPort> onConnectionFailed;

    private final InvocationResponseFuture future = new InvocationResponseFuture();

    private AtomicBoolean started = new AtomicBoolean();
    private AtomicReference<Throwable> lastException = new AtomicReference<>();

    // current outstanding task for debugging
    private AtomicReference<ListenableFuture<?>> currentTask = new AtomicReference<>();

    InvocationAttempt(
            List<HostAndPort> addresses,
            NiftyConnectionManager niftyConnectionManager,
            InvocationFunction<NiftyConnection> invocationFunction,
            Consumer<HostAndPort> onConnectionFailed)
    {
        this.addresses = ImmutableList.copyOf(addresses).iterator();
        this.niftyConnectionManager = niftyConnectionManager;
        this.invocationFunction = invocationFunction;
        this.onConnectionFailed = onConnectionFailed;
    }

    ListenableFuture<Object> getFuture()
    {
        if (started.compareAndSet(false, true)) {
            try {
                tryNextAddress();
            }
            catch (Throwable throwable) {
                future.fatalError(throwable);
            }
        }

        return future;
    }

    private void tryNextAddress()
    {
        // request was already canceled
        if (future.isCancelled()) {
            return;
        }

        if (!addresses.hasNext()) {
            future.fatalError(new TTransportException(NOT_OPEN, "No hosts available", lastException.get()));
            return;
        }

        HostAndPort address = addresses.next();
        ListenableFuture<NiftyConnection> connectionFuture = niftyConnectionManager.getConnection(address);
        currentTask.set(connectionFuture);
        Futures.addCallback(connectionFuture, new SafeFutureCallback<NiftyConnection>()
        {
            @Override
            public void safeOnSuccess(NiftyConnection connection)
            {
                tryInvocation(connection, address);
            }

            @Override
            public void safeOnFailure(Throwable t)
            {
                lastException.set(t);

                onConnectionFailed.accept(address);

                tryNextAddress();
            }
        });
    }

    private void tryInvocation(NiftyConnection connection, HostAndPort address)
    {
        try {
            ListenableFuture<Object> invocationFuture = invocationFunction.invokeOn(connection);
            currentTask.set(invocationFuture);
            Futures.addCallback(invocationFuture, new SafeFutureCallback<Object>()
            {
                @Override
                public void safeOnSuccess(Object result)
                {
                    future.success(result);
                    niftyConnectionManager.returnConnection(connection);
                }

                @Override
                public void safeOnFailure(Throwable t)
                {
                    if (invocationFunction.isHostDownException(t)) {
                        onConnectionFailed.accept(address);
                    }
                    niftyConnectionManager.returnConnection(connection);

                    if (invocationFunction.isRetryable(t)) {
                        lastException.set(t);
                        tryNextAddress();
                    }
                    else {
                        future.fatalError(t);
                    }
                }
            });
        }
        catch (Throwable e) {
            niftyConnectionManager.returnConnection(connection);
            throw e;
        }
    }

    // This is an non-static inner class so it retains a reference to
    // the invocation attempt which make debugging easier
    private class InvocationResponseFuture
            extends AbstractFuture<Object>
    {
        void success(Object value)
        {
            set(value);
        }

        void fatalError(Throwable throwable)
        {
            if (throwable instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                throwable = new TException(throwable);
            }

            // exception in the future is expected to be a TException
            if (!(throwable instanceof TException)) {
                throwable = new TException(throwable);
            }
            setException(throwable);
        }
    }

    // The invocation attempt works by using repeated callbacks without any active monitoring, so
    // it is critical that callbacks do not throw.  This is a safety system to turn programming
    // errors into a failed future so, the request doesn't hang.
    private abstract class SafeFutureCallback<T>
            implements FutureCallback<T>
    {
        abstract void safeOnSuccess(@Nullable T result)
                throws Exception;

        abstract void safeOnFailure(Throwable throwable)
                throws Exception;

        @Override
        public final void onSuccess(@Nullable T result)
        {
            try {
                safeOnSuccess(result);
            }
            catch (Throwable t) {
                future.fatalError(t);
            }
        }


        @Override
        public final void onFailure(Throwable throwable)
        {
            try {
                safeOnFailure(throwable);
            }
            catch (Throwable t) {
                future.fatalError(t);
            }
        }
    }
}
