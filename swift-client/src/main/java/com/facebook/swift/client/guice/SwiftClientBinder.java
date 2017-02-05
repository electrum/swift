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
package com.facebook.swift.client.guice;

import com.facebook.swift.client.SwiftClient;
import com.facebook.swift.client.SwiftClientFactory;
import com.facebook.swift.transport.AddressSelector;
import com.facebook.swift.transport.ClientEventHandler;
import com.facebook.swift.transport.MethodInvoker;
import com.facebook.swift.transport.SwiftClientConfig;
import com.facebook.swift.transport.guice.MethodInvokerFactory;
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

import static com.facebook.swift.client.ThriftServiceMetadata.getThriftServiceAnnotation;
import static com.facebook.swift.client.guice.SwiftClientAnnotationFactory.getSwiftClientAnnotation;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class SwiftClientBinder
{
    public static final String DEFAULT_NAME = "default";

    public static SwiftClientBinder swiftClientBinder(Binder binder)
    {
        return new SwiftClientBinder(binder);
    }

    private final Binder binder;

    private SwiftClientBinder(Binder binder)
    {
        this.binder = binder.skipSources(this.getClass());
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

        configBinder(binder).bindConfig(SwiftClientConfig.class, clientAnnotation, configPrefix);

        TypeLiteral<SwiftClient<T>> typeLiteral = swiftClientTypeLiteral(clientInterface);

        Provider<T> instanceProvider = new SwiftClientInstanceProvider<T>(clientAnnotation, Key.get(typeLiteral, annotation));
        Provider<SwiftClient<T>> factoryProvider = new SwiftClientProvider<>(clientInterface, clientAnnotation);

        binder.bind(Key.get(clientInterface, annotation)).toProvider(instanceProvider).in(Scopes.SINGLETON);
        binder.bind(Key.get(typeLiteral, annotation)).toProvider(factoryProvider).in(Scopes.SINGLETON);

        if (annotation == DefaultClient.class) {
            binder.bind(Key.get(clientInterface)).toProvider(instanceProvider).in(Scopes.SINGLETON);
            binder.bind(Key.get(typeLiteral)).toProvider(factoryProvider).in(Scopes.SINGLETON);
        }

//        jmxExport(binder, key, typeName, clientName);

        return new SwiftClientBindingBuilder(binder, clientAnnotation, configPrefix);
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
            extends AbstractAnnotatedProvider<T>
    {
        private final Key<SwiftClient<T>> key;

        public SwiftClientInstanceProvider(Annotation annotation, Key<SwiftClient<T>> key)
        {
            super(annotation);
            this.key = requireNonNull(key, "key is null");
        }

        @Override
        protected T get(Injector injector, Annotation annotation)
        {
            return injector.getInstance(key).get();
        }
    }

    private static class SwiftClientProvider<T>
            extends AbstractAnnotatedProvider<SwiftClient<T>>
    {
        private final Class<T> clientInterface;

        public SwiftClientProvider(Class<T> clientInterface, Annotation annotation)
        {
            super(annotation);
            this.clientInterface = requireNonNull(clientInterface, "clientInterface is null");
        }

        @Override
        protected SwiftClient<T> get(Injector injector, Annotation annotation)
        {
            MethodInvokerFactory methodInvokerFactory = injector.getInstance(MethodInvokerFactory.class);
            SwiftClientFactory proxyFactory = injector.getInstance(SwiftClientFactory.class);
            AddressSelector addressSelector = injector.getInstance(Key.get(AddressSelector.class, annotation));

            List<ClientEventHandler<?>> eventHandlers = ImmutableList.copyOf(injector.getInstance(
                    Key.get(new TypeLiteral<Set<ClientEventHandler<?>>>() {}, annotation)));

            MethodInvoker invoker = methodInvokerFactory.createMethodInvoker(addressSelector, annotation);

            return proxyFactory.createSwiftClient(invoker, clientInterface, eventHandlers);
        }
    }
}
