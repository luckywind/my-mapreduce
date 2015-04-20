package neu.mapreduce.core.factory;

/**
 * Created by srikar on 4/20/15.
 */

import api.JobConf;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

public final class JobConfFactory<T extends JobConf>{

    private Class<T> typeArgumentClass;
    private T singletonObject;

    // creates instance
    public JobConfFactory(Class<T> typeArgumentClass) throws IllegalAccessException, InstantiationException {

        this.typeArgumentClass = typeArgumentClass;
        singletonObject = typeArgumentClass.newInstance();
    }

    // creates instance using urlClassLoader
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

    public T getSingletonObject() {
        return singletonObject;
    }

    public T getNewInstance() throws IllegalAccessException, InstantiationException {
        T myNewT = typeArgumentClass.newInstance();
        myNewT.initialize();
        return myNewT;
    }
}
