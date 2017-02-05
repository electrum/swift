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

import com.facebook.swift.transport.ClientEventHandler;
import com.google.inject.Binder;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;

import java.lang.annotation.Annotation;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.util.Objects.requireNonNull;

public class SwiftClientBindingBuilder
{
    private final Binder binder;
    private final Annotation annotation;
    private final String prefix;

    SwiftClientBindingBuilder(Binder binder, Annotation annotation, String prefix)
    {
        this.binder = binder.skipSources(getClass());
        this.annotation = requireNonNull(annotation, "annotation is null");
        this.prefix = requireNonNull(prefix, "prefix is null");
        eventBinder(); // initialize binding
    }

    public SwiftClientBindingBuilder withEventHandler(Class<? extends ClientEventHandler<?>> eventHandler)
    {
        eventBinder().addBinding().to(eventHandler);
        return this;
    }

    public SwiftClientBindingBuilder withAddressSelector(AddressSelectorBinder selectorBinder)
    {
        selectorBinder.bind(binder, annotation, prefix);
        return this;
    }

    private Multibinder<ClientEventHandler<?>> eventBinder()
    {
        return newSetBinder(binder, new TypeLiteral<ClientEventHandler<?>>() {}, annotation);
    }
}
