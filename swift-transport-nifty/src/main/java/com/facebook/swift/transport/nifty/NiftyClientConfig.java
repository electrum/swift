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

import com.google.common.net.HostAndPort;
import io.airlift.configuration.Config;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class NiftyClientConfig
{
    private int maxFrameSize = toIntExact(new DataSize(16, MEGABYTE).toBytes());
    private Duration connectTimeout = new Duration(500, MILLISECONDS);
    private Duration receiveTimeout = new Duration(1, MINUTES);
    private Duration readTimeout = new Duration(10, SECONDS);
    private Duration writeTimeout = new Duration(1, MINUTES);
    private HostAndPort socksProxy;
    private boolean poolEnabled;

    @MinDuration("1ms")
    public Duration getConnectTimeout()
    {
        return connectTimeout;
    }

    @Config("thrift.client.connect-timeout")
    public NiftyClientConfig setConnectTimeout(Duration connectTimeout)
    {
        this.connectTimeout = connectTimeout;
        return this;
    }

    @MinDuration("1ms")
    public Duration getReceiveTimeout()
    {
        return receiveTimeout;
    }

    @Config("thrift.client.receive-timeout")
    public NiftyClientConfig setReceiveTimeout(Duration receiveTimeout)
    {
        this.receiveTimeout = receiveTimeout;
        return this;
    }

    @MinDuration("1ms")
    public Duration getReadTimeout()
    {
        return readTimeout;
    }

    @Config("thrift.client.read-timeout")
    public NiftyClientConfig setReadTimeout(Duration readTimeout)
    {
        this.readTimeout = readTimeout;
        return this;
    }

    @MinDuration("1ms")
    public Duration getWriteTimeout()
    {
        return writeTimeout;
    }

    @Config("thrift.client.write-timeout")
    public NiftyClientConfig setWriteTimeout(Duration writeTimeout)
    {
        this.writeTimeout = writeTimeout;
        return this;
    }

    public HostAndPort getSocksProxy()
    {
        return socksProxy;
    }

    @Config("thrift.client.socks-proxy")
    public NiftyClientConfig setSocksProxy(HostAndPort socksProxy)
    {
        this.socksProxy = socksProxy;
        return this;
    }

    @Min(0)
    @Max(0x3FFFFFFF)
    public int getMaxFrameSize()
    {
        return maxFrameSize;
    }

    @Config("thrift.client.max-frame-size")
    public NiftyClientConfig setMaxFrameSize(int maxFrameSize)
    {
        checkArgument(maxFrameSize <= 0x3FFFFFFF);
        this.maxFrameSize = maxFrameSize;
        return this;
    }

    public boolean isPoolEnabled()
    {
        return poolEnabled;
    }

    @Config("thrift.client.pool-enabled")
    public void setPoolEnabled(boolean poolEnabled)
    {
        this.poolEnabled = poolEnabled;
    }
}
