package mx.cinvestav.config

import scala.collection.immutable.IntMap
case class CacheNode(host:String,port:Int){
  def url = s"http://$host:$port"
}
case class DefaultConfig(
                          nodeId:String,
                          sourceFolder:String,
                          sinkFolder:String,
                          staticExtension:String,
                          poolUrl:String,
                          workloadFolder:String,
                          maxConcurrent:Int,
                          writeOnDisk:Boolean,
                          consumers:Int,
                          role:String,
                          consumerIndex:Int,
                          consumerPort:Int,
//                          level:String,
                          producerIndex:Int,
                          numFiles:Int,
                          maxDurationMs:Long,
                          seed:Int,
                          paretoShape:Double,
                          paretoScale:Double,
                          maxDownloads:Int,
                          consumerIterations:Int,
                          writeDebug:Boolean,
                          readDebug:Boolean,
                          fromConsumerFile:Boolean,
                          producerRate:Long,
                          producerMode:String,
                          consumerMode:String,
                          consumerRate:Long,
                          appMode:String,
                          requestTimeoutMs:Long,
                          connectTimeoutMs:Long,
                          maxWaitQueueLimit:Int,
                          maxTotalConnections:Int,
                          idleTimeout:Long,
                          bufferSize:Int,

                        )
