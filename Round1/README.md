1.环境搭建

	Yarn的安装前的准备
		安装Open JDK 1.7
			apt-get install openjdk-7-jdk
			java-version
		OpenSSH
			apt-get install ssh
			apt-get install rsync
		创建hadoop组
			addgroup hadoop
		创建用户hadoop并添加到hadoop组中
			adduser --ingroup hadoop hadoop
		配置SSH的免密登录
			检查hduser是否可以免密方式登录localhost
			ssh localhost
		生成公钥：
			ssh-keygen -t dsa
			cat ~/.ssh/id_dsa.pub >>  ~/.ssh/authorized_keys
		配置hadoop环境变量
			文件解压目录下的/etc/hadoop/hadoop-env.sh
			设置Java Home：export JAVA_HOME=/usr/lib/jvm/
			设置hadoop prifix：export HADOOP_PREFIX=/hadoop/hadoop
		测试Hadoop
			bin/hadoop
		更改主机名
			sudo vi /etc/hostname
			把Ubuntu改为master，便于区分master和slave
		添加域名，方便通过域名连接主机，这里我使用伪分布式的方式配置Hadoop，所以我只配置master的IP
			sudo vi /etc/hosts
			添加内容为：192.168.2.228 master
			
	Hadoop的伪分布式
		1./etc/hadoop/core-site.xml
		fs.defaultFS配置NameNode的URI
		<configuration>
			<property>
				<name>fs.defaultFS</name>
				<value>hdfs://master:9000</value>
			</property>
		</configuration>
		
		2./etc/hadoop/hdfs-site.xml
		dfs.replication设置块的复制数量
		<configuration>
			<property>
				<name>dfs.replication</name>
				<value>1</value>
			</property>
		</configuration>
		
		3./etc/hadoop/mapred-site.xml
		mapreduce.framework.name配置MapReduce应用使用Yarn框架
		<configuration>
			<property>
				<name>mapreduce.framework.name</name>
				<value>yarn</value>
			</property>
		</configuration>
		
		4./etc/hadoop/yarn-site.xml
		yarn.nodemanager.aux-services：为NodeManager配置MapReduce应用的Shuffle服务
		<configuration>
			<property>
				<name>yarn.nodemanager.aux-services</name>
				<value>mapreduce_shuffle</value>
			</property>
		</configuration>
		
		5./etc/hadoop/slaves
			master
		
2.数据处理
	
	每天的UV统计
	map：
		输入：key为行号，value为每行的数据
		输出：key为日期，value为IP
	reduce:
		输出：key为日期，value为IP数
	对日志文件进行分析，可以确定所访问过网站的不同IP为不同用户，对UV进行统计，即对同一天内不同的IP进行统计，由于相同的IP有可能在一天内多次访问网站，访问记录同时也会保存在日志文件，所以我们需要对相同的IP地址进行去重，在reduce处理时，可以定义一个set集合用于存储ip，实现自动去重
	
	每天访问量Top10的Show统计
	map：
		输入：key为行号，value为每行的数据
		输出：key为日期，value为IP
	reduce：
		输出：key为日期，value为Top10的show
	由于相同的页面同样存在相同用户访问多次，所以通过MapReduce编程框架实现具体的功能，在Reduce进行归约操作的时候，需要定义一个临时set集合存储IP，集合set的大小即为IP的数量，也就是该Show页面的访问量，然后拼接show和ip数量，通过ArrayList添加，然后自定义排序，最后遍历ArrayLsit前十条数据，将Show页面的访问路径和IP地址输出保存。
	
	次日留存统计
	假设某天
	map：
		输入：key为行号，value为每行的数据
		输出：key为日期，value为IP
	reduce：
		输出：key为日期，value为IP
	假设set1存储这天的ip
	同样对于次日
	map：
		输入：key为行号，value为每行的数据
		输出：key为日期，value为IP
	reduce：
		输出：key为日期，value为IP
	假设set2存储次日的ip
	使用set3循环判断set2.contains(s)，如果存在则add，最后set3里面所有元素为某天与次日都访问过的ip，set3.size()/set1.size()可以看作是次日的留存率。
	
	
	
	
	
	
	
	
	
	
	
	
	
	
