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

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.thrift.transport.TTransportException;

import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.apache.thrift.transport.TTransportException.NOT_OPEN;

public class NiftyConnectionPool
        implements NiftyConnectionManager, Closeable
{
    private final NiftyConnectionManager connectionFactory;

    private final LoadingCache<HostAndPort, ListenableFuture<NiftyConnection>> cachedConnections;
    private final ScheduledExecutorService maintenanceThread;

    @GuardedBy("this")
    private boolean closed;

    public NiftyConnectionPool(NiftyConnectionManager connectionFactory, NiftyClientConfig config)
    {
        this.connectionFactory = connectionFactory;
        requireNonNull(config, "config is null");

        // todo from config
        cachedConnections = CacheBuilder.newBuilder()
                .maximumSize(100)
                .expireAfterAccess(10, TimeUnit.MINUTES)
                .<HostAndPort, ListenableFuture<NiftyConnection>>removalListener(notification -> closeConnection(notification.getValue()))
                .build(new CacheLoader<HostAndPort, ListenableFuture<NiftyConnection>>()
                {
                    @Override
                    public ListenableFuture<NiftyConnection> load(HostAndPort address)
                            throws Exception
                    {
                        return createConnection(address);
                    }
                });

        maintenanceThread = newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                .setNameFormat("nifty-connection-maintenance-%s")
                .setDaemon(true)
                .build());

        maintenanceThread.scheduleWithFixedDelay(cachedConnections::cleanUp, 1, 1, TimeUnit.SECONDS);
    }

    public ListenableFuture<NiftyConnection> getConnection(HostAndPort address)
    {
        ListenableFuture<NiftyConnection> future;
        synchronized (this) {
            if (closed) {
                return Futures.immediateFailedFuture(new TTransportException(NOT_OPEN, "Connection pool is closed"));
            }

            try {
                future = cachedConnections.get(address);
            }
            catch (ExecutionException | UncheckedExecutionException | ExecutionError e) {
                throw Throwables.propagate(e.getCause());
            }
        }

        // check if connection is failed
        if (isFailed(future)) {
            // remove failed connection
            cachedConnections.asMap().remove(address, future);
        }
        return future;
    }

    @Override
    public void returnConnection(NiftyConnection connection)
    {
    }

    @Override
    public synchronized void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        try {
            cachedConnections.invalidateAll();
        }
        finally {
            maintenanceThread.shutdownNow();
        }
    }

    private ListenableFuture<NiftyConnection> createConnection(HostAndPort address)
    {
        return connectionFactory.getConnection(address);
    }

    private void closeConnection(ListenableFuture<NiftyConnection> futureConnection)
    {
        Futures.addCallback(futureConnection, new FutureCallback<NiftyConnection>()
        {
            @Override
            public void onSuccess(NiftyConnection result)
            {
                result.close();
            }

            @Override
            public void onFailure(Throwable ignored)
            {
                // connection was never opened
            }
        });
    }

    private static boolean isFailed(ListenableFuture<?> future)
    {
        if (!future.isDone()) {
            return false;
        }
        try {
            future.get();
            return false;
        }
        catch (Exception e) {
            return true;
        }
    }
}
