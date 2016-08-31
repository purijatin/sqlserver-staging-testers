package com.kilo;

import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.utility.JavaModule;

import static net.bytebuddy.matcher.ElementMatchers.*;


public class AgentSetup {
    public static void setup(){
        System.out.println("\n\n\n\n\n\n\nDone\n\n\n\n\n");
        new AgentBuilder.Default()
                .type(declaresMethod(nameStartsWith("getTypeHandler")))
                .transform((builder, typeDescription, classLoader) ->
                        builder
                                .method(any())
                                .intercept(MethodDelegation.to(MonitorInterceptor.class))
                )
                .with(new AgentBuilder.Listener() {
                    @Override
                    public void onTransformation(TypeDescription typeDescription, ClassLoader classLoader, JavaModule javaModule, DynamicType dynamicType) {

                    }

                    @Override
                    public void onIgnored(TypeDescription typeDescription, ClassLoader classLoader, JavaModule javaModule) {

                    }

                    @Override
                    public void onError(String s, ClassLoader classLoader, JavaModule javaModule, Throwable throwable) {

                    }

                    @Override
                    public void onComplete(String s, ClassLoader classLoader, JavaModule javaModule) {

                    }
                })
                .installOnByteBuddyAgent();
    }
}
