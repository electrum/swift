/*
 * Copyright (C) 2012 Facebook, Inc.
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

import com.facebook.swift.transport.AddressSelector;
import com.facebook.swift.transport.MethodInvoker;
import com.facebook.swift.transport.guice.MethodInvokerFactory;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.apache.thrift.async.TAsyncClientManager;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.Closeable;
import java.io.IOException;
import java.lang.annotation.Annotation;

public class ApacheThriftMethodInvokerFactory
        implements MethodInvokerFactory, Closeable
{
    private final Injector injector;
    private final TAsyncClientManager asyncClientManager;

    @Inject
    public ApacheThriftMethodInvokerFactory(Injector injector)
    {
        this.injector = injector;
        try {
            this.asyncClientManager = new TAsyncClientManager();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MethodInvoker createMethodInvoker(AddressSelector addressSelector, Annotation qualifier)
    {
        ApacheThriftClientConfig config = injector.getInstance(Key.get(ApacheThriftClientConfig.class, qualifier));
        return new ApacheThriftMethodInvoker(asyncClientManager, addressSelector, config);
    }

    @PreDestroy
    @Override
    public void close()
    {
        asyncClientManager.stop();
    }
}
