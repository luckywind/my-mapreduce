package neu.mapreduce.core.factory;

import api.MyMapper;
import api.MyReducer;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * Created by mit,srikar,vishal on 4/8/15.
 */

public final class ReducerFactory<T extends MyReducer>{

    private Class<T> typeArgumentClass;
    private T singletonObject;

    public ReducerFactory(Class<T> typeArgumentClass) throws IllegalAccessException, InstantiationException {

        this.typeArgumentClass = typeArgumentClass;
        singletonObject = typeArgumentClass.newInstance();
    }

    public ReducerFactory(String clientJarPath, String reducerClassname) throws MalformedURLException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

        File aFile = new File(clientJarPath);
        URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{aFile.toURI().toURL()});
        Class<?> reducerClass = urlClassLoader.loadClass(reducerClassname);
        Class<? extends MyReducer> clientMapperClass = reducerClass.asSubclass(MyReducer.class);

        Constructor<? extends MyReducer> clientConstructor = clientMapperClass.getConstructor();
        MyReducer myReducer = clientConstructor.newInstance();
        singletonObject = (T) myReducer;
    }


    public T getSingletonObject() {
        return singletonObject;
    }

    public T getNewInstance() throws IllegalAccessException, InstantiationException {
        T myNewT = typeArgumentClass.newInstance();
        return myNewT;
    }
}