package org.unidal.cat.message.config;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

public abstract class ContainerHolder implements ApplicationContextAware {

    private ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    protected <T> T lookup(Class<T> clazz) {
        return this.applicationContext.getBean(clazz);
    }

    protected <T> T lookup(Class<T> beanClazz, String beanId) {
        return this.applicationContext.getBean(beanId, beanClazz);
    }


    public void release(Object obj) {

    }
}
