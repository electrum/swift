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

import com.facebook.swift.client.AddressSelector;
import com.facebook.swift.client.ClientEventHandler;
import com.facebook.swift.client.MethodInvoker;
import com.facebook.swift.client.SwiftClient;
import com.facebook.swift.client.SwiftClientFactory;
import com.facebook.swift.service.ThriftClientConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import org.weakref.jmx.ObjectNameBuilder;

import javax.inject.Provider;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Set;

import static com.facebook.swift.client.guice.SwiftClientAnnotationFactory.getSwiftClientAnnotation;
import static com.facebook.swift.service.ThriftClientManager.DEFAULT_NAME;
import static com.facebook.swift.service.metadata.ThriftServiceMetadata.getThriftServiceAnnotation;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class SwiftClientBinder
{
    public static SwiftClientBinder swiftClientBinder(Binder binder)
    {
        return new SwiftClientBinder(binder);
    }

    private final Binder binder;

    private SwiftClientBinder(Binder binder)
    {
        this.binder = requireNonNull(binder, "binder is null").skipSources(this.getClass());
    }

    public <T> SwiftClientBindingBuilder bindSwiftClient(Class<T> clientInterface)
    {
        String typeName = getServiceName(clientInterface);

        return bindSwiftClient(clientInterface, typeName, DEFAULT_NAME, typeName, DefaultClient.class);
    }

    public <T> SwiftClientBindingBuilder bindSwiftClient(Class<T> clientInterface, Class<? extends Annotation> annotationType)
    {
        String typeName = getServiceName(clientInterface);
        String clientName = annotationType.getSimpleName();
        String configPrefix = typeName + "." + clientName;

        return bindSwiftClient(clientInterface, typeName, clientName, configPrefix, annotationType);
    }

    private <T> SwiftClientBindingBuilder bindSwiftClient(
            Class<T> clientInterface,
            String typeName,
            String clientName,
            String configPrefix,
            Class<? extends Annotation> annotation)
    {
        binder.bind(SwiftClientFactory.class).in(Scopes.SINGLETON);

        Annotation clientAnnotation = getSwiftClientAnnotation(clientInterface, annotation);

        configBinder(binder).bindConfig(ThriftClientConfig.class, clientAnnotation, configPrefix);

        TypeLiteral<SwiftClient<T>> typeLiteral = swiftClientTypeLiteral(clientInterface);

        Provider<T> instanceProvider = new SwiftClientInstanceProvider<>(clientInterface, annotation);
        Provider<SwiftClient<T>> factoryProvider = new SwiftClientProvider<>(clientInterface, annotation);

        binder.bind(Key.get(clientInterface, annotation)).toProvider(instanceProvider).in(Scopes.SINGLETON);
        binder.bind(Key.get(typeLiteral, annotation)).toProvider(factoryProvider).in(Scopes.SINGLETON);

        if (annotation == DefaultClient.class) {
            binder.bind(Key.get(clientInterface)).toProvider(instanceProvider).in(Scopes.SINGLETON);
            binder.bind(Key.get(typeLiteral)).toProvider(factoryProvider).in(Scopes.SINGLETON);
        }

//        jmxExport(binder, key, typeName, clientName);

        return new SwiftClientBindingBuilder(
                newSetBinder(binder, new TypeLiteral<ClientEventHandler<?>>() {}, clientAnnotation));
    }

    private static void jmxExport(Binder binder, Key<?> key, String typeName, String clientName)
    {
        newExporter(binder)
                .export(key)
                .as(new ObjectNameBuilder("com.facebook.swift.client")
                        .withProperty("type", typeName)
                        .withProperty("clientName", clientName)
                        .build());
    }

    private static String getServiceName(Class<?> clientInterface)
    {
        requireNonNull(clientInterface, "clientInterface is null");
        String serviceName = getThriftServiceAnnotation(clientInterface).value();
        if (!serviceName.isEmpty()) {
            return serviceName;
        }
        return clientInterface.getSimpleName();
    }

    @SuppressWarnings("unchecked")
    private static <T> TypeLiteral<SwiftClient<T>> swiftClientTypeLiteral(Class<T> clientInterface)
    {
        Type javaType = new TypeToken<SwiftClient<T>>() {}
                .where(new TypeParameter<T>() {}, TypeToken.of(clientInterface))
                .getType();
        return (TypeLiteral<SwiftClient<T>>) TypeLiteral.get(javaType);
    }

    private static class SwiftClientInstanceProvider<T>
            extends AbstractAnnotatedProvider<T, T>
    {
        public SwiftClientInstanceProvider(Class<T> clientInterface, Class<? extends Annotation> annotation)
        {
            super(clientInterface, annotation);
        }

        @Override
        protected T get(Injector injector, Class<T> clientInterface, Class<? extends Annotation> annotation)
        {
            return injector.getInstance(Key.get(swiftClientTypeLiteral(clientInterface), annotation)).get();
        }
    }

    private static class SwiftClientProvider<T>
            extends AbstractAnnotatedProvider<T, SwiftClient<T>>
    {
        public SwiftClientProvider(Class<T> clientInterface, Class<? extends Annotation> annotation)
        {
            super(clientInterface, annotation);
        }

        @Override
        protected SwiftClient<T> get(Injector injector, Class<T> clientInterface, Class<? extends Annotation> annotation)
        {
            MethodInvokerFactory methodInvokerFactory = injector.getInstance(MethodInvokerFactory.class);
            SwiftClientFactory proxyFactory = injector.getInstance(SwiftClientFactory.class);

            Annotation clientAnnotation = getSwiftClientAnnotation(clientInterface, annotation);

            AddressSelector addressSelector = injector.getInstance(Key.get(AddressSelector.class, clientAnnotation));
            List<ClientEventHandler<?>> eventHandlers = ImmutableList.copyOf(injector.getInstance(
                    Key.get(new TypeLiteral<Set<ClientEventHandler<?>>>() {}, clientAnnotation)));

            MethodInvoker invoker = methodInvokerFactory.createMethodInvoker(addressSelector, clientAnnotation);
            return proxyFactory.createSwiftClient(invoker, clientInterface, eventHandlers);
        }
    }
}
