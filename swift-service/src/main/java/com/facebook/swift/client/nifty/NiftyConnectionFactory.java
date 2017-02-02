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

import com.facebook.nifty.client.NiftyClient;
import com.facebook.nifty.client.NiftyClientChannel;
import com.facebook.nifty.client.NiftyClientConnector;
import com.facebook.swift.client.AddressSelector;
import com.facebook.swift.service.ThriftClientConfig;
import com.google.common.base.Function;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import org.apache.thrift.transport.TTransportException;

import static java.util.Objects.requireNonNull;
import static org.apache.thrift.transport.TTransportException.NOT_OPEN;

public class NiftyConnectionFactory
        implements NiftyConnectionManager
{
    private final NiftyClient niftyClient;
    private final NiftyClientConnectorFactory niftyClientConnectorFactory;
    private final AddressSelector addressSelector;

    private final Duration connectTimeout;
    private final Duration receiveTimeout;
    private final Duration readTimeout;
    private final Duration writeTimeout;
    private final int maxFrameSize;
    private final HostAndPort socksProxy;

    @Inject
    public NiftyConnectionFactory(
            NiftyClient niftyClient,
            NiftyClientConnectorFactory niftyClientConnectorFactory,
            AddressSelector addressSelector,
            ThriftClientConfig config)
    {
        this.niftyClient = requireNonNull(niftyClient, "niftyClient is null");
        this.niftyClientConnectorFactory = requireNonNull(niftyClientConnectorFactory, "niftyClientConnectorFactory is null");
        this.addressSelector = requireNonNull(addressSelector, "addressSelector is null");

        requireNonNull(config, "config is null");
        this.connectTimeout = config.getConnectTimeout();
        this.receiveTimeout = config.getReceiveTimeout();
        this.readTimeout = config.getReadTimeout();
        this.writeTimeout = config.getWriteTimeout();
        this.maxFrameSize = config.getMaxFrameSize();
        this.socksProxy = config.getSocksProxy();
    }

    @Override
    public ListenableFuture<NiftyConnection> getConnection(HostAndPort address)
    {
        try {
            NiftyClientConnector<NiftyClientChannel> connector = niftyClientConnectorFactory.createConnector(address);
            ListenableFuture<NiftyClientChannel> connectFuture = niftyClient.connectAsync(
                    connector,
                    connectTimeout,
                    receiveTimeout,
                    readTimeout,
                    writeTimeout,
                    maxFrameSize,
                    socksProxy);

            return Futures.transform(connectFuture, (Function<NiftyClientChannel, NiftyConnection>)
                    channel -> new NiftyConnection(channel, createNiftyConnectionContext(address, channel)));
        }
        catch (RuntimeException e) {
            // nifty sometimes throws runtime exceptions on connect
            return Futures.immediateFailedFuture(new TTransportException(NOT_OPEN, e));
        }
    }

    @Override
    public void returnConnection(NiftyConnection connection)
    {
        connection.close();
    }

    private NiftyConnectionContext createNiftyConnectionContext(HostAndPort address, NiftyClientChannel channel)
    {
        return new NiftyConnectionContext(channel.getNettyChannel().getRemoteAddress(), () -> addressSelector.markdown(address));
    }
}
