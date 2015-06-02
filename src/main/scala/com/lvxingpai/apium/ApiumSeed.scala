package com.lvxingpai.apium

import java.util.UUID

import com.fasterxml.jackson.databind.node._
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}

/**
 * 包装一个Celery task的Json数据结构
 *
 * Created by zephyre on 5/27/15.
 */
case class ApiumSeed(node: ObjectNode) {
  override def toString: String = {
    val mapper = new ObjectMapper()
    mapper.writeValueAsString(node)
  }
}

object ApiumSeed {
  def apply(task: String,
            args: Option[Seq[JsonNode]] = None,
            kwargs: Option[Map[String, JsonNode]] = None,
            expire: Option[DateTime] = None,
            eta: Option[DateTime] = None,
            timelimit: Option[(Float, Float)] = None): ApiumSeed = {

    val mapper = new ObjectMapper()
    val node = mapper.createObjectNode()

    Seq("taskset", "errbacks", "callbacks", "chord") foreach (node.set(_, NullNode.getInstance()))

    node.set("utc", BooleanNode.getTrue)
    node.put("task", task)
    node.put("id", UUID.randomUUID().toString)
    node.put("retries", 0)

    // 处理args
    val argsNode = mapper.createArrayNode()
    args.getOrElse(Seq[JsonNode]()) foreach argsNode.add
    node.set("args", argsNode)

    // 处理kwargs
    val kwargsNode = mapper.createObjectNode()
    kwargs.getOrElse(Map[String, JsonNode]()) foreach (item => kwargsNode.set(item._1, item._2))
    node.set("kwargs", kwargsNode)

    // 处理timelimit
    val tlNode = mapper.createArrayNode()
    val tlValues = timelimit map (item => (FloatNode.valueOf(item._1), FloatNode.valueOf(item._2))) getOrElse
      (NullNode.getInstance(), NullNode.getInstance())
    tlNode.add(tlValues._1)
    tlNode.add(tlValues._2)
    node.set("timelimit", tlNode)

    // 将Option[DateTime]转换为ValueNode（包括TextNode和NullNode）
    def dt2textNode(someDt: Option[DateTime]): ValueNode = {
      someDt map (
        dt => {
          val utcDt = dt.withZone(DateTimeZone.UTC)
          val dtString = ISODateTimeFormat.dateTime().print(utcDt)
          TextNode.valueOf(dtString)
        }) getOrElse NullNode.getInstance()
    }

    node.set("expires", dt2textNode(expire))
    node.set("eta", dt2textNode(eta))

    new ApiumSeed(node)
  }
}
