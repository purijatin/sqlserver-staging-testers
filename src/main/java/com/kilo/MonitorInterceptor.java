package com.kilo;

import java.util.HashSet;
import java.util.concurrent.Callable;

import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;


/**
 * Created by puri on 8/31/16.
 */
public class MonitorInterceptor {


    public static int count = 0;
    static HashSet<String> set =  new HashSet<String>();

    @RuntimeType
    public static Object intercept(@Origin String method,
                                   @SuperCall Callable<?> zuper) throws Exception {
        try {
            if(set.add(method))
                System.out.println("Intercepting for methods: "+method);
            return zuper.call();
        } catch (Exception ex){
            ex.printStackTrace();
            throw ex;
        }finally {
            count++;
            if(count !=0 && count %1000_000 == 0)
                System.out.println(count);
        }
    }
}
