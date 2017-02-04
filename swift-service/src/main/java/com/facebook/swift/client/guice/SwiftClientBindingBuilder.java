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
package com.facebook.swift.client.guice;

import com.facebook.swift.client.ClientEventHandler;
import com.google.inject.multibindings.Multibinder;

import static java.util.Objects.requireNonNull;

public class SwiftClientBindingBuilder
{
    private final Multibinder<ClientEventHandler<?>> clientEventHandlers;

    public SwiftClientBindingBuilder(Multibinder<ClientEventHandler<?>> clientEventHandlers)
    {
        this.clientEventHandlers = requireNonNull(clientEventHandlers, "clientEventHandlers is null");
    }

    public SwiftClientBindingBuilder withEventHandler(Class<? extends ClientEventHandler<?>> eventHandler)
    {
        clientEventHandlers.addBinding().to(eventHandler);
        return this;
    }

//    public SwiftClientBindingBuilder withAddressSelector(Class<? extends AddressSelector> addressSelector)
//    {
//        return this;
//    }
//
//    public SwiftClientBindingBuilder withAddressSelector(Class<? extends Annotation> annotationType)
//    {
//        return withAddressSelector(Key.get(AddressSelector.class, annotationType));
//    }
//
//    public SwiftClientBindingBuilder withAddressSelector(Key<?> key)
//    {
//        return this;
//    }
}
