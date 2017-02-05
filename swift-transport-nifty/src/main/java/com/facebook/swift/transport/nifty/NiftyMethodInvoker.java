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

import com.facebook.swift.transport.AddressSelector;
import com.facebook.swift.transport.ClientEventHandler;
import com.facebook.swift.transport.MethodInvoker;
import com.facebook.swift.transport.MethodMetadata;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class NiftyMethodInvoker
        implements MethodInvoker
{
    private final AddressSelector addressSelector;
    private final NiftyConnectionManager connectionManager;

    @Inject
    public NiftyMethodInvoker(NiftyConnectionManager connectionManager, AddressSelector addressSelector)
    {
        this.addressSelector = requireNonNull(addressSelector, "addressSelector is null");
        this.connectionManager = requireNonNull(connectionManager, "connectionManager is null");
    }

    @Override
    public ListenableFuture<Object> invoke(
            MethodMetadata method,
            List<ClientEventHandler<?>> handlers,
            Optional<String> addressSelectionContext,
            Map<String, String> headers,
            List<Object> parameters)
    {
        try {
            List<HostAndPort> addresses = addressSelector.getAddresses(addressSelectionContext);
            InvocationAttempt invocationAttempt = new InvocationAttempt(
                    addresses,
                    connectionManager,
                    connection -> invoke(connection, handlers, method, parameters),
                    addressSelector::markdown);
            return invocationAttempt.getFuture();
        }
        catch (Exception e) {
            return Futures.immediateFailedFuture(e);
        }
    }

    private ListenableFuture<Object> invoke(NiftyConnection connection, List<ClientEventHandler<?>> handlers, MethodMetadata method, List<Object> parameters)
    {
        try {
            // todo make a pool of these and reuse?
            NiftyProtocol niftyProtocol = new NiftyProtocol(connection.getProtocolFactory());

            return connection.invoke(niftyProtocol, handlers, method, parameters);
        }
        catch (Throwable throwable) {
            if (throwable instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                throwable = new TException(throwable);
            }
            return Futures.immediateFailedFuture(throwable);
        }
    }
}
