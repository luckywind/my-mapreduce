package neu.mapreduce.core.factory;

import api.MyMapper;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * Created by mit, srikar, vishal on 4/8/15.
 */


public final class MapFactory<T extends MyMapper>{

    private Class<T> typeArgumentClass;
    private T singletonObject;

    public MapFactory(Class<T> typeArgumentClass) throws IllegalAccessException, InstantiationException {

        this.typeArgumentClass = typeArgumentClass;
        singletonObject = typeArgumentClass.newInstance();
    }


    public MapFactory(String clientJarPath, String mapperClassname) throws MalformedURLException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

        File aFile = new File(clientJarPath);
        URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{aFile.toURI().toURL()});
        Class<?> mapperClass = urlClassLoader.loadClass(mapperClassname);
        Class<? extends MyMapper> clientMapperClass = mapperClass.asSubclass(MyMapper.class);

        Constructor<? extends MyMapper> clientConstructor = clientMapperClass.getConstructor();
        MyMapper myMapper = clientConstructor.newInstance();
        singletonObject = (T) myMapper;

    }


    public T getSingletonObject() {
        return singletonObject;
    }

    public T getNewInstance() throws IllegalAccessException, InstantiationException {
        T myNewT = typeArgumentClass.newInstance();
        return myNewT;
    }
}