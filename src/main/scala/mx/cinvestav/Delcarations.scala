package mx.cinvestav

object Delcarations{
  case class Upload(userId:String,operation:String,fileId:String,fileSize:Double,userRole:String)
  case class Download(userId:String,operation:String,fileId:String,fileSize:Double)
}
