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
package com.facebook.swift.client;

import com.facebook.swift.client.StatsClientEventHandler.PerCallMethodStats;
import com.facebook.swift.service.ThriftMethodStats;
import com.facebook.swift.transport.ClientEventHandler;
import com.facebook.swift.transport.ConnectionContext;
import io.airlift.units.Duration;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static io.airlift.units.Duration.nanosSince;
import static java.lang.System.nanoTime;

public class StatsClientEventHandler
        implements ClientEventHandler<PerCallMethodStats>
{
    private final ConcurrentHashMap<String, ThriftMethodStats> stats = new ConcurrentHashMap<>();

    static class PerCallMethodStats
    {
        private long startTime = nanoTime();
        private long preReadTime;
        private long postReadTime;
        private long preWriteTime;
    }

    private static Duration nanosBetween(long start, long end)
    {
        return new Duration(end - start, TimeUnit.NANOSECONDS);
    }

    public ConcurrentMap<String, ThriftMethodStats> getStats()
    {
        return stats;
    }

    @Override
    public PerCallMethodStats getContext(String methodName, ConnectionContext context)
    {
        stats.putIfAbsent(methodName, new ThriftMethodStats());
        return new PerCallMethodStats();
    }

    @Override
    public void preWrite(PerCallMethodStats context, String methodName, List<Object> parameters)
    {
        long now = nanoTime();
        context.preWriteTime = now;
        stats.get(methodName).addInvokeTime(nanosBetween(context.postReadTime, now));
    }

    @Override
    public void postWrite(PerCallMethodStats context, String methodName, List<Object> result)
    {
        stats.get(methodName).addWriteTime(nanosSince(context.preWriteTime));
//        stats.get(methodName).addWriteByteCount(getBytesWritten(context));
    }

    @Override
    public void preRead(PerCallMethodStats context, String methodName)
    {
        context.preReadTime = nanoTime();
    }

    @Override
    public void postRead(PerCallMethodStats context, String methodName, Object result)
    {
        long now = nanoTime();
        context.postReadTime = now;
        stats.get(methodName).addReadTime(nanosBetween(context.preReadTime, now));
//        stats.get(methodName).addReadByteCount(getBytesRead(context));

        stats.get(methodName).addSuccessTime(nanosSince(context.startTime));
    }

    @Override
    public void postReadException(PerCallMethodStats context, String methodName, Throwable t)
    {
        // do not record read time because error might skew the results
        stats.get(methodName).addErrorTime(nanosSince(context.startTime));
    }

//    private int getBytesRead(PerCallMethodStats stats)
//    {
//        if (!(stats.requestContext instanceof NiftyRequestContext)) {
//            return 0;
//        }
//
//        NiftyRequestContext requestContext = (NiftyRequestContext) stats.requestContext;
//        return requestContext.getNiftyTransport().getReadByteCount();
//    }
//
//    private int getBytesWritten(PerCallMethodStats stats)
//    {
//        if (!(ctx.requestContext instanceof NiftyRequestContext)) {
//            // Standard TTransport interface doesn't give us a way to determine how many bytes
//            // were read
//            return 0;
//        }
//
//        NiftyRequestContext requestContext = (NiftyRequestContext) ctx.requestContext;
//        return requestContext.getNiftyTransport().getWrittenByteCount();
//    }
}
