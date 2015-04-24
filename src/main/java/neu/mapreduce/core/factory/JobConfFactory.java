package neu.mapreduce.core.factory;

/**
 * Created by Mit, Srikar on 4/20/15.
 */

import api.JobConf;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * Factory which creates {@link api.JobConf} subclass instances at runtime. It creates
 * instance only once and returns same instance if called again.
 *
 * @param <T> creates instances of class T which extends {@link api.JobConf}
 */

public final class JobConfFactory<T extends JobConf> {

    private Class<T> typeArgumentClass;
    private T singletonObject;

    /**
     * Constructor which creates instance using class object
     *
     * @param typeArgumentClass Class whose instance need to be created
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public JobConfFactory(Class<T> typeArgumentClass) throws IllegalAccessException, InstantiationException {

        this.typeArgumentClass = typeArgumentClass;
        singletonObject = typeArgumentClass.newInstance();
    }

    /**
     * Constructor which creates instance of the given jobConfClassname which is loaded from the
     * clientJar at runtime using {@link java.net.URLClassLoader}. It also invokes a method
     * in {@JobConf} which initializes the job configuration
     *
     * @param clientJarPath    client jar path which has client code
     * @param jobConfClassname name of the class whose instance need to be created
     * @throws MalformedURLException
     * @throws ClassNotFoundException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws InstantiationException
     */
    public JobConfFactory(String clientJarPath, String jobConfClassname) throws MalformedURLException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

        File aFile = new File(clientJarPath);
        URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{aFile.toURI().toURL()});
        Class<?> jobConfClass = urlClassLoader.loadClass(jobConfClassname);
        Class<? extends JobConf> clientJobConfClass = jobConfClass.asSubclass(JobConf.class);

        Constructor<? extends JobConf> clientConstructor = clientJobConfClass.getConstructor();
        JobConf jobConf = clientConstructor.newInstance();
        singletonObject = (T) jobConf;
        singletonObject.initialize();
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
        myNewT.initialize();
        return myNewT;
    }
}
