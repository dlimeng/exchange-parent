统一引擎和中间件服务

运行方式

direct-runner（只适合测试，资源受限）:

java -cp jarUrl main --参数1名称=值1...

spark-runner:

spark-submit  --class main  --master spark://HOST:PORT jarUrl --runner=SparkRunner --参数1名称=值1...

例1
spark-submit  --class com.knowlegene.parent.process.SwapAndSparkApplication \
 --master yarn --deploy-mode cluster  \
 --jars /usr/hdp/2.6.5.0-292/hive2/lib/hive-service-2.1.0.2.6.5.0-292.jar  \
 /mnt/shell/kd-process-1.0-SNAPSHOT-shaded.jar   --runner=SparkRunner  --sparkMaster=local[
*] --bundleSize=1024

idea插件
安装Lombok

私服配置
	
     <mirror>
      <id>alimaven</id>
      <name>aliyun maven</name>
      <url>http://maven.aliyun.com/nexus/content/groups/public/</url>
      <mirrorOf>central</mirrorOf>        
     </mirror> 

     <mirror>  
      <id>repo2</id>  
      <mirrorOf>central</mirrorOf>  
      <name>Human Readable Name for this Mirror.</name>  
      <url>http://repo2.maven.org/maven2/</url>  
    </mirror>
    
    

