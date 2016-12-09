统计需求
1.统计每日、周、月的UV数量

	UV数量：
		map：
		输出：
			key：ip  
			value：1
		reduce：
		输出：
			key：date 
			value：count
			
	数据来源：每天的数据为以天命名的一个文件，每周的数据为连续7天的数据，每月    的数据为以月份命名的文件夹里的数据，那么统计每日、周、月的UV数量可以通过控制输入的原始文件来完成相应的需求
	数据处理：
		map：
			1.输出ip \t 1
		reduce：
			1.定义静态成员变量count，每调用一次reduce()方法,count++
			2.输出 ip \t count
	数据输出：Reducer中，通过cleanup()方法一次性输出date \t count
		
	
2.统计每日的PV量

	PV数量：
		map：
		输出：
			key：date
			value：page
		输出：
			key：date 
			value：count
			
	数据来源：每天的数据
	数据处理：在Reducer中，定义一个静态类成员count，在reduce()方法中对page数进行累加，cleanup()一次性输出PV量
	数据输出：Reducer中，通过cleanup()方法一次性输出date \t count

3.统计次日留存、次月留存

	留存：
		map：
		输出：
			key：ip
			value：1 | 2
		reduce：
		输出：
			key：date
			value：count
			
	数据来源：某一天的数据和下一天的数据   或   某一月的数据和下一月的数据
	数据处理：
		map：
			1.判断输入数据的文件来源
			2.标记输出value，1为某一天，2为下一天
		reduce：
			1.标记ip访问状态，value = 1，exits1 = true；value = 2，exits2 = true；
			2.exits1 && exits2为true，count++
	数据输出：Reducer中，通过cleanup()方法一次性输出date \t count，date为下一天
	
4.统计每类网页的跳转率
	
	跳转率：
		map：
			key：ip
			value：1
		reduce：
			key：date
			value：count / sum
	
	数据来源：某类网页的数据
	数据处理：
		map：
			1.通过正则表达式匹配某类的网页
			2.输出ip \t 1
		reduce：
			1.定义静态成员变量sum，统计该类网页的UV总数
			2.定义静态成员变量count，统计ip访问页面超过1的UV数
			3.输出date \t count/sum
	数据输出：在Reducer中，通过cleanup()方法，一次性输出
		
5.统计每天从baidu跳转过来的PV

	PV量：
		map：
		输出：
			key：baidu
			value：page
		reduce：
			key：date
			value：count
	
	数据来源：某一天的数据
	数据处理：
		map：
			1.通过正则表达式匹配从baidu跳转的访问记录
			2.输出baidu \t page
		reduce：
			1.对page进行累加
			2.输出date \t count，date为某一天
	数据输出：在Reducer中，通过cleanup()方法，一次性输出
	
6.统计每天iOS和Android的UV数

	UV数：
		map：
			key：ip
			value：1
		reduce：
			key：date
			value：count
		
	数据来源：某一天的数据
	数据处理：
		map：
			1.通过正则表达式匹配通过IOS或Android设备访问的记录
			2.输出ip \t 1
		reduce：
			1.定义静态成员变量count，每调用一次reduce()方法，count++
			2.输出date \t count，date为某一天
	数据输出：在Reducer中，通过cleanup()方法，一次性输出
	
实时需求

1.查询当前的show的访问数量

	(1) 通过mapreduce计算框架过滤输出show，ip，count
	(2) 以show为rowkey，ip为列，count位列建show表，每次添加相同rowkey的ip时，在原来的列值追加，通过空格分隔，count++
	(3) 从HBase中查询相应的show时，可以直接读取列为count的数据，该数据即为当前show的访问数量

2.查询当前的musician的访问数量

	(1) 通过mapreduce计算框架过滤输出musician，ip，count
	(2) 以musician为rowkey，ip为列，count位列建musician表，每次添加相同rowkey的ip时，在原来的列值追加，通过空格分隔，count++
	(3) 从HBase中查询相应的musician时，可以直接读取列为count的数据，该数据即为当前musician的访问数量
	
项目拆解

2016-12-09至2016-12-10：完成统计需求1、2
2016-12-10至2016-12-11：完成统计需求3、4
2016-12-11至2016-12-12：完成统计需求5、6
2016-12-12至2016-12-13：完成实时需求1、2
