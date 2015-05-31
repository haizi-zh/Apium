package com.lvxingpai.apium

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode

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
