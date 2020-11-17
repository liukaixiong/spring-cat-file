package com.cat.file.message.config;

import com.cat.file.message.MessageManagerProcess;
import com.cat.file.message.config.props.StorageProperties;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

@Configurable
@EnableConfigurationProperties(StorageProperties.class)
@ComponentScan(value = "com.cat.file.message")
public class CatFileConfiguration {

    @Bean
    public MessageManagerProcess messageManagerProcess() {
        return new MessageManagerProcess();
    }

}
