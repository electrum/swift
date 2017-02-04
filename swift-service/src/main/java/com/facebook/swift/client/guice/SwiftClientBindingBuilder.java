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
