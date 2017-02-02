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
import com.facebook.swift.client.guice.MethodInvokerFactory;
import com.facebook.swift.service.ThriftClientConfig;
import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.configuration.ConfigBinder;
import io.airlift.configuration.ConfigurationBinding;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class NiftyClientModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(NiftyClient.class).in(SINGLETON);
        binder.bind(MethodInvokerFactory.class).to(NiftyMethodInvokerFactory.class).in(SINGLETON);

        configBinder(binder).bindConfigurationBindingListener(this::bindNiftyClientConfig);
    }

    private void bindNiftyClientConfig(ConfigurationBinding<?> binding, ConfigBinder configBinder)
    {
        if (binding.getConfigClass().equals(ThriftClientConfig.class)) {
            configBinder.bindConfig(NiftyClientConfig.class, binding.getKey().getAnnotation(), binding.getPrefix().orElse(null));
        }
    }
}
