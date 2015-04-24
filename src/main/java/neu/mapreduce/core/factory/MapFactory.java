package neu.mapreduce.core.factory;

import api.MyMapper;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * Created by Mit, Srikar on 4/8/15.
 */

/**
 * Factory which creates {@link api.MyMapper} subclass instances at runtime. It creates
 * instance only once and returns same instance if called again.
 *
 * @param <T> creates instances of class T which extends {@link api.MyMapper}
 */

public final class MapFactory<T extends MyMapper> {

    private Class<T> typeArgumentClass;
    private T singletonObject;

    /**
     * Constructor which creates instance using class object
     *
     * @param typeArgumentClass Class whose instance need to be created
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public MapFactory(Class<T> typeArgumentClass) throws IllegalAccessException, InstantiationException {
        this.typeArgumentClass = typeArgumentClass;
        singletonObject = typeArgumentClass.newInstance();
    }

    /**
     * Constructor which creates instance of the given mapperClassname which is loaded from the
     * clientJar at runtime using {@link java.net.URLClassLoader}
     *
     * @param clientJarPath   client jar path which has client code
     * @param mapperClassname name of the class whose instance need to be created
     * @throws MalformedURLException
     * @throws ClassNotFoundException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws InstantiationException
     */
    public MapFactory(String clientJarPath, String mapperClassname) throws MalformedURLException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

        File aFile = new File(clientJarPath);
        URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{aFile.toURI().toURL()});
        Class<?> mapperClass = urlClassLoader.loadClass(mapperClassname);
        Class<? extends MyMapper> clientMapperClass = mapperClass.asSubclass(MyMapper.class);

        Constructor<? extends MyMapper> clientConstructor = clientMapperClass.getConstructor();
        MyMapper myMapper = clientConstructor.newInstance();
        singletonObject = (T) myMapper;
    }

    /**
     * @return singleton object
     */
    public T getSingletonObject() {
        return singletonObject;
    }

    /**
     * Creates new instance of the given class
     *
     * @return a new instance of given class
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public T getNewInstance() throws IllegalAccessException, InstantiationException {
        T myNewT = typeArgumentClass.newInstance();
        return myNewT;
    }
}