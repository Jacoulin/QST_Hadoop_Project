# QST_Hadoop_Project

## 项目背景

某电商平台XD，每天有大量的用户访问，XD的产品经理ZB，想了解现在网站的访问情况，于是提出了一个需求

1. 需要看到网站的访问情况
    1. 包括PV、UV
2. 需要符合大数据的概念
3. 必须支持后的开发需求，要尽量高的开发效率

## 项目开展

本次项目的负责人是：

项目分4个阶段开展

## Round 1

本回合完成任务主要完成：

1. 完成设计方案
    1. 方案内容填写在./Round1/README.md 文件里
    2. 方案包含所有操作的内容，由环境搭建，到最后的代码运行
2. 完成环境搭建
    1. 建议进行操作一遍
3. 完成以下统计需求
    1. 完成每天的UV统计
    2. 完成每天访问量Top10的Show统计
    3. 完成每天的次日留存统计

相关资料完成后，请发起pull request，里面标题填上自己的名字。

数据在客户端本地磁盘 /home/hadoop/qst/ray/2015-1\* ， 在HDFS上也有 /user/hadoop/hadoop_project/*

---

## Round 2 - Start From the Beginning

有一家电商公司，委托了我们来做一个项目，项目目标是搭建一个日志处理系统，定期地收集日志，并且对日志进行统计分析，产出对该网站的统计报表。并且提供一些线上的。

现在陈经理带领大家一块来进行这个项目。首先，大家的技术都是Hadoop相关的，所以我们这一次做的项目，也是用Hadoop的技术来实现。

---
我们的实施过程，会划分为几个部分

1. 需求摸底：了解清楚这个日志统计平台，需要的是什么样的功能
2. 方案设计：根据我们接触到的需求，我们进行方案设计，让对方知道我们的系统，是由什么组成，怎么工作，如何提供服务的。
3. 项目拆解：根据方案，我们把这个项目，分解成多个小项目，进行时间安排。
4. 项目实施：根据拆解的内容，我们一一进行编码，实现每个小项目。
5. 项目验收：对我们的系统进行验收，由对方判断是否满足他们的需求。

---
由于陈经理已经做好了客户的沟通，『需求摸底』已经完成了。我们从『方案设计』开始。

以下是对方的需求：

1. 必须能安全地存储这些日志，不能够丢失。
2. 需要对这些日志进行统计，得到网站的访问情况。
3. 需要提供实时的查询服务，提供给外部的服务使用。

其中，统计需求有以下这些：

1. 统计每日、周、月的UV数量。
2. 统计每日的PV量。
3. 统计次日留存、次月留存。
4. 统计每类网页的跳转率。
5. 统计每天从baidu跳转过来的PV
6. 统计每天iOS和Android的UV数
7. 统计每天各个省份的UV数量（可以使用`https://www.juhe.cn/docs/api/id/1` 来获取IP的地域信息）
8. 统计每天每个省份访问的show的Top-5（按照UV量）


实时的查询服务需求：

1. 查询当前的show的访问数量
2. 查询当前的musician的访问数量

---

以下，请大家完成剩下的部分，陈经理会回答过程中的问题。请大家前面所提到的：

* 方案设计——提交方案，方案中包括系统如何设计，包含哪些模块，为什么选用这些模块。
* 项目拆解——把需求安排在一定的时间内完成，给出项目各个阶段和小项目的完成时间。
* 项目实施——实现需求，把项目的实施代码，上传到github。

