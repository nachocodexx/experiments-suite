package mx.cinvestav.config

import scala.collection.immutable.IntMap
case class CacheNode(host:String,port:Int){
  def url = s"http://$host:$port"
}
case class DefaultConfig(
                          nodeId:String,
                          loadBalancer:String,
                          workloadPath:String,
                          workloadFilename:String,
                          cacheNodes:List[CacheNode],
                          sourceFolder:String,
                          staticExtension:String,
                          rabbitmq: RabbitMQClusterConfig,
                        )
