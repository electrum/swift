package com.facebook.swift.client.guice;

import com.facebook.nifty.client.NiftyClient;
import com.facebook.swift.client.AddressSelector;
import com.facebook.swift.client.ClientEventHandler;
import com.facebook.swift.client.SwiftClient;
import com.facebook.swift.client.SwiftClientFactory;
import com.facebook.swift.client.nifty.FramedNiftyClientConnectorFactory;
import com.facebook.swift.client.nifty.NiftyConnectionFactory;
import com.facebook.swift.client.nifty.NiftyConnectionManager;
import com.facebook.swift.client.nifty.NiftyMethodInvoker;
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
        this.binder = requireNonNull(binder, "binder is null");
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
            NiftyClient niftyClient = injector.getInstance(NiftyClient.class);
            SwiftClientFactory proxyFactory = injector.getInstance(SwiftClientFactory.class);

            Annotation clientAnnotation = getSwiftClientAnnotation(clientInterface, annotation);

            ThriftClientConfig clientConfig = injector.getInstance(Key.get(ThriftClientConfig.class, clientAnnotation));
            AddressSelector addressSelector = injector.getInstance(Key.get(AddressSelector.class, clientAnnotation));
            List<ClientEventHandler<?>> eventHandlers = ImmutableList.copyOf(injector.getInstance(
                    Key.get(new TypeLiteral<Set<ClientEventHandler<?>>>() {}, clientAnnotation)));

            NiftyConnectionManager connectionManager = new NiftyConnectionFactory(
                    niftyClient,
                    new FramedNiftyClientConnectorFactory(),
                    addressSelector,
                    clientConfig);

            NiftyMethodInvoker invoker = new NiftyMethodInvoker(connectionManager, addressSelector);

            return proxyFactory.createSwiftClient(invoker, clientInterface, eventHandlers);
        }
    }
}
