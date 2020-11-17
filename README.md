## [spring-cat-file](https://github.com/liukaixiong/spring-cat-file)

基于 [cat-hadoop](https://github.com/dianping/cat/tree/master/cat-hadoop)改造而来，主要是想做一些本地文件文本数据存储的东西，项目中使用到了CAT，然后它的本地存储功能很强大，量大存储的空间小，觉得很棒，但是由于是**内部项目**,很多依赖和Spring不兼容，想用到它的 功能得进行改造，这也是这个项目产生的目的。

目前核心流程已经跑通，接下来就大概讲下使用的方式吧。

### 应用背景

- 有一些不固定文本数据存储到数据库的话，首先数据长度就很头疼个，时不时字段不够，一旦量大的话，千万级几十个G，尽管大部分时候不需要查询，只是再出问题的时候能够定位到该文本数据。

- 上面的问题用ELK也能够解决，把日志打印出来做收集就行了，但是一旦量大的话成本也是很高（本身ELK就是一个成本很高的东西），公司还是希望尽可能的简单，轻便就能够达到目的就行了。
- 搜索交给`Mysql`或者其他搜索引擎，大文本内容存入本地文件，尽可能的让这一部分数据有途径被找到。存储大小也能尽可能的被压缩。

## 1. 引入依赖:

```xml
<dependency>
    <groupId>com.cat.file.message</groupId>
    <artifactId>spring-cat-file</artifactId>
    <version>1.0-SNAPSHOT</version>
    <exclusions>
        <exclusion>
            <artifactId>servlet-api</artifactId>
            <groupId>javax.servlet</groupId>
        </exclusion>
    </exclusions>
</dependency>
```

## 2. 存储消息

```java
// 直接引入
@Autowired
private MessageManagerProcess messageManagerProcess;

// 将消息存入
MessageTree messageTree = new DefaultMessageTree(应用名称,应用ip, 递增编号, 创建时间, 存储的内容);
this.messageManagerProcess.insert(messageTree);

// 拿到对应的消息编号，这个是在插入之前就已经构建好了
String messageId = messageTree.getMessageId()
```

## 3. 查询消息

```java
// 引入消息管理器
@Autowired
private MessageManagerProcess messageManagerProcess;

// 根据LogId定位到消息
String msgContent = messageManagerProcess.getMessage(logId);

// 转换成任意对象，这里的对象和存储的时候尽量保持一致，最好都用过json
String messageObject = messageManagerProcess.getMessageObject(logId, String.class);
```

## 4. 消息落盘

```java
@Autowired
private MessageManagerProcess messageManagerProcess;

// 落盘当前所有内存中的数据
messageManagerProcess.closeAll();

// 落盘指定小时的
messageManagerProcess.close(445999);
```



## 简单代码介绍

消息处理一共分为了三个层面:

**消息处理器**

`MessageDumperManager` : 负责开启多线程管理多个队列去处理客户端发送过来的消息

`MessageProcessor` :  消息处理器，所有消息都会被该类处理，不过都是先放入内存中。通过触发定时操作落盘

**内存消息管理**

`MessageFinderManager` : 由于当前小时被处理的消息会在内存中找到，所以这相当于第一级缓存

**本地文件存储管理器**

`BucketManager` : 这个的类负责和本地文件进行映射管理，所有基于本地文件的操作都由该类处理

## 注意事项

### 消息处理过程

通过消息管理器插入一条消息的时候，是先存储到内存中，然后有一个异步线程定时的将上一个小时的数据进行落盘到磁盘文件中，如果存在重启的话，最好是先落盘所有数据。

## 关于更多的参考

[参考cat服务端](https://github.com/dianping/cat/wiki/server)

[王亚普的存储文章](https://blog.csdn.net/yapuge/article/details/89703952)

[存储描述](https://cloud.tencent.com/developer/article/1428969)







