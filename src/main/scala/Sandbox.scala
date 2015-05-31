import com.fasterxml.jackson.databind.node.TextNode
import com.lvxingpai.apium.{ApiumPlant, ApiumSeedBuilder}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * Created by zephyre on 5/27/15.
 */
object Sandbox extends App {
  override def main(args: Array[String]): Unit = {
    val plant = ApiumPlant("192.168.100.2", 5672, "guest", "guest")

    val opsSeq = Seq(
      ApiumPlant.declareExchange("yunkai", ApiumPlant.EXCHANGE_DIRECT, durable = true),
      ApiumPlant.declareExchange("yunkai.createUser", ApiumPlant.EXCHANGE_FANOUT, durable = true),
      ApiumPlant.bindExchange("yunkai.createUser", "yunkai", "createUser"),
      ApiumPlant.declareQueue("yunkai.createUserWelcome"),
      ApiumPlant.bindQueue("yunkai.createUserWelcome", "yunkai.createUser")
    )

    plant.connect(ApiumPlant.mergeOps(opsSeq: _*))

    Future {
      def loop(n: Long): Unit = {
        println(s"Loo: #$n")

        val seed = ApiumSeedBuilder("yunkai.createUser", Some(Seq(TextNode.valueOf("Zephyre")))).build()
        plant.sendSeed("yunkai", "createUser", seed)
        //        plant.sendMessage("yunkai", "createUser", "haha")

        Thread.sleep(1000)
        loop(n + 1)
      }
      loop(0)
    }
  }


  main(args)
}
