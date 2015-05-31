package com.lvxingpai.apium

import java.net.URLEncoder
import java.util.UUID

import akka.actor.{ActorRef, ActorSystem}
import com.rabbitmq.client.ConnectionFactory
import com.thenewmotion.akka.rabbitmq._

import scala.concurrent.duration._

/**
 * 基于akka库，对RabbitMQ的封装。可以异步地设置RabbitMQ，包括exchange，queue等。支持将一个Apium Seed投递到RabbitMQ队列中。
 *
 * Created by zephyre on 5/27/15.
 */
class ApiumPlant(val plantId: String, val host: String, val port: Int, val user: String, passwd: String,
                 val virtualHost: String = "/") {

  implicit private val actorSystem = ActorSystem()

  private val connActor = {
    val factory = new ConnectionFactory()
    val virtualHostEnc = URLEncoder.encode(virtualHost, "utf-8")
    factory.setUri(s"amqp://$user:$passwd@$host:$port/$virtualHostEnc")
    actorSystem.actorOf(ConnectionActor.props(factory), "rabbitmq")
  }

  def connect(ops: ApiumPlant.ConnectionOps = null): Unit = if (ops!=null)
    connActor ! CreateChannel(ChannelActor.props(ops), Some("channelActor"))
  else
    connActor ! CreateChannel(ChannelActor.props(), Some("channelActor"))

  /**
   * 向RabbitMQ发送一个ApiumSeed
   *
   * @param exchange
   * @param routingKey
   * @param seed
   */
  def sendSeed(exchange: String, routingKey: String, seed: ApiumSeed): Unit =
    sendMessage(exchange, routingKey, seed.toString)

  /**
   * 向RabbitMQ发送一条消息
   *
   * @param exchange
   * @param routingKey
   * @param message
   */
  def sendMessage(exchange: String, routingKey: String, message: String): Unit =
    sendMessage(exchange, routingKey, message.getBytes("utf-8"))

  /**
   * 向RabbitMQ发送一条消息
   *
   * @param exchange
   * @param routingKey
   * @param body
   */
  def sendMessage(exchange: String, routingKey: String, body: Array[Byte]): Unit = {
    val channelActor = actorSystem.actorSelection("/user/rabbitmq/channelActor")

    channelActor ! ChannelMessage((channel: Channel) => channel.basicPublish(exchange, routingKey, null, body),
      dropIfNoChannel = false)
  }

  /**
   * 关闭ApiumPlant
   */
  def shutdown(): Unit = {
    actorSystem.awaitTermination(30 seconds)
    ApiumPlant -= plantId
  }
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
   * 从ApiumPlant中去除某个plant
   *
   * @param plantID
   */
  def removePlantByID(plantID: String): Unit = plantMap -= plantID

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

  def apply(host: String, port: Int, user: String, passwd: String, virtualHost: String = "/"): ApiumPlant = {
    val plantID = UUID.randomUUID().toString
    val plant = new ApiumPlant(plantID, host, port, user, passwd, virtualHost)
    ApiumPlant.plantMap(plantID) = plant
    plant
  }

  /**
   * 关闭ApiumPlant
   *
   * @param plantID
   */
  def shutdown(plantID: String): Unit = {
    val plant = ApiumPlant(plantID)
    if (plant nonEmpty)
      plant.get.shutdown()
  }

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
}
