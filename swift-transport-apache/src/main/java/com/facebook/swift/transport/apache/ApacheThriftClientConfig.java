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
package com.facebook.swift.transport.apache;

import io.airlift.configuration.Config;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.TimeUnit.MINUTES;

public class ApacheThriftClientConfig
{
    private int maxFrameSize = toIntExact(new DataSize(16, MEGABYTE).toBytes());
    private int maxStringSize = toIntExact(new DataSize(16, MEGABYTE).toBytes());
    private Duration timeout = new Duration(1, MINUTES);

    @MinDuration("1ms")
    public Duration getTimeout()
    {
        return timeout;
    }

    @Config("thrift.client.timeout")
    public void setTimeout(Duration timeout)
    {
        this.timeout = timeout;
    }

    @Min(0)
    @Max(0x3FFFFFFF)
    public int getMaxStringSize()
    {
        return maxStringSize;
    }

    @Config("thrift.client.max-string-size")
    public ApacheThriftClientConfig setMaxStringSize(int maxFrameSize)
    {
        checkArgument(maxFrameSize <= 0x3FFFFFFF);
        this.maxStringSize = maxFrameSize;
        return this;
    }

    @Min(0)
    @Max(0x3FFFFFFF)
    public int getMaxFrameSize()
    {
        return maxFrameSize;
    }

    @Config("thrift.client.max-frame-size")
    public ApacheThriftClientConfig setMaxFrameSize(int maxFrameSize)
    {
        checkArgument(maxFrameSize <= 0x3FFFFFFF);
        this.maxFrameSize = maxFrameSize;
        return this;
    }
}
