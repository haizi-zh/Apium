package com.lvxingpai.apium

import java.net.URLEncoder
import java.util.UUID

import akka.actor.{ActorRef, ActorSystem}
import com.lvxingpai.apium.ApiumPlant.ConnectionParam
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.ConnectionFactory
import com.thenewmotion.akka.rabbitmq._

import scala.collection.JavaConversions._
import scala.concurrent.duration._

import scala.language.postfixOps

/**
 * 基于akka库，对RabbitMQ的封装。可以异步地设置RabbitMQ，包括exchange，queue等。支持将一个Apium Seed投递到RabbitMQ队列中。
 *
 * Created by zephyre on 5/27/15.
 */
class ApiumPlant(connParam: ConnectionParam, val apiumName: String, val pods: Seq[String] = Seq()) {

  val plantId = UUID.randomUUID().toString

  implicit private val actorSystem = ActorSystem()

  private val connActor = {
    val factory = new ConnectionFactory()
    val virtualHostEnc = URLEncoder.encode(connParam.virtualHost, "utf-8")
    val user = connParam.user
    val passwd = connParam.passwd
    val host = connParam.host
    val port = connParam.port
    factory.setUri(s"amqp://$user:$passwd@$host:$port/$virtualHostEnc")
    actorSystem.actorOf(ConnectionActor.props(factory), "rabbitmq")
  }

  {
    // 构造连接RabbitMQ时需要声明的exchange
    val entryExchangeOps = ApiumPlant.declareExchange(apiumName, ApiumPlant.EXCHANGE_DIRECT, durable = true)
    val seedExchangeOps = pods flatMap (podName => {
      val name = s"$apiumName.$podName"
      val seedExchange = ApiumPlant.declareExchange(name, ApiumPlant.EXCHANGE_FANOUT, durable = true)
      val bindSeed = ApiumPlant.bindExchange(name, apiumName, podName)
      Seq(seedExchange, bindSeed)
    })

    val ops = entryExchangeOps +: seedExchangeOps

    // 建立连接
    connActor ! CreateChannel(ChannelActor.props(ApiumPlant.mergeOps(ops: _*)), Some("channelActor"))
  }

  /**
   * 根据podName，生成默认情况下的taskName
   *
   * @param podName
   * @return
   */
  def defaultTaskName(podName: String): String = {
    if (pods contains podName) {
      val eventName = podName.capitalize
      s"$apiumName.on$eventName"
    } else throw new IllegalArgumentException(s"Pod name $podName does not exist.")
  }

  /**
   * 向RabbitMQ发送一个ApiumSeed
   *
   * @param podName
   * @param seed
   */
  def sendSeed(podName: String, seed: ApiumSeed): Unit = {
    val builder = new BasicProperties.Builder()
    builder.correlationId(seed.uuid)
      .priority(0)
      .deliveryMode(2)
      .headers(Map[String, AnyRef]())
      .contentEncoding("utf-8")
      .contentType("application/json")

    sendMessage(apiumName, seed.toString, props = builder.build(), routingKey = podName)
  }

  def sendMessage(exchange: String, message: String, props: BasicProperties = null, routingKey: String = ""): Unit = {
    val channelActor = actorSystem.actorSelection("/user/rabbitmq/channelActor")

    channelActor ! ChannelMessage((channel: Channel) => channel.basicPublish(exchange, routingKey, props,
      message.getBytes("utf-8")), dropIfNoChannel = false)
  }

  /**
   * 关闭ApiumPlant
   */
  def shutdown(): Unit = actorSystem.awaitTermination(30 seconds)
}

object ApiumPlant {
  type ConnectionOps = (Channel, ActorRef) => Any
  val EXCHANGE_DIRECT = "direct"
  val EXCHANGE_FANOUT = "fanout"

  private val plantMap = scala.collection.mutable.Map[String, ApiumPlant]()

  /**
   * 获得所有的ApiumPlant ID列表
   * @return
   */
  def plantList: Set[String] = plantMap.keySet.toSet

  /**
   * 从ApiumPlant中去除某个plant
   *
   * @param plantID
   */
  def -=(plantID: String): Unit = removePlantByID(plantID)

  /**
   * Declare an exchange in RabbitMQ
   * @param name
   * @param exchangeType
   * @param durable
   * @param autoDelete
   * @return
   */
  def declareExchange(name: String, exchangeType: String, durable: Boolean = false,
                      autoDelete: Boolean = false): ConnectionOps =
    (channel: Channel, _: ActorRef) => channel.exchangeDeclare(name, exchangeType, durable, autoDelete, null)

  /**
   * Bind two exchanges together
   * @param dst
   * @param src
   * @param routingKey
   * @return
   */
  def bindExchange(dst: String, src: String, routingKey: String): ConnectionOps =
    (channel: Channel, _: ActorRef) => channel.exchangeBind(dst, src, routingKey)

  /**
   * Declare a queue in RabbitMQ
   * @param name
   * @param durable
   * @param exclusive
   * @param autoDelete
   * @return
   */
  def declareQueue(name: String, durable: Boolean = false, exclusive: Boolean = false, autoDelete: Boolean = false):
  ConnectionOps = (channel: Channel, _: ActorRef) => channel.queueDeclare(name, durable, exclusive, autoDelete, null)

  /**
   * Bind a queue to an exchange
   * @param queue
   * @param exchange
   * @param routingKey
   * @return
   */
  def bindQueue(queue: String, exchange: String, routingKey: String = ""): ConnectionOps =
    (channel: Channel, _: ActorRef) => channel.queueBind(queue, exchange, routingKey)

  /**
   * 将多项ConnectionOps合并为一个ConnectionOps
   *
   * @param ops
   * @return
   */
  def mergeOps(ops: ConnectionOps*): ConnectionOps =
    (channel: Channel, self: ActorRef) => ops foreach (_(channel, self))

  def apply(connParam: ConnectionParam, apiumName: String, pods: Seq[String] = Seq()): ApiumPlant = {
    val plant = new ApiumPlant(connParam, apiumName, pods = pods)
    ApiumPlant.plantMap(plant.plantId) = plant
    plant
  }

  /**
   * 关闭ApiumPlant
   *
   * @param plantID
   */
  def shutdown(plantID: String): Unit = {
    val plant = ApiumPlant(plantID)
    if (plant nonEmpty) {
      plant.get.shutdown()
      ApiumPlant.removePlantByID(plantID)
    }
  }

  /**
   * 从ApiumPlant中去除某个plant
   *
   * @param plantID
   */
  def removePlantByID(plantID: String): Unit = plantMap -= plantID

  /**
   * 根据PlantID获得一个ApiumPlant实例
   *
   * @param plantID
   * @return
   */
  def apply(plantID: String): Option[ApiumPlant] = getPlantByID(plantID)

  /**
   * 根据PlantID获得一个ApiumPlant实例
   *
   * @param plantID
   * @return
   */
  def getPlantByID(plantID: String): Option[ApiumPlant] = plantMap.get(plantID)

  /**
   * 连接参数
   */
  case class ConnectionParam(host: String, port: Int, user: String, passwd: String, virtualHost: String = "/")

}
