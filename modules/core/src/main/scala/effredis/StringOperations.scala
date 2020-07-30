package effredis

import cats.effect._

import serialization._
import algebra.StringApi
import StringApi._

import scala.concurrent.duration.Duration

class StringOperations[F[_]: Concurrent: ContextShift]
  (implicit blocker: Blocker) 
  extends StringApi[F] with Redis { 

  def set(
    key: Any, 
    value: Any, 
    whenSet: SetBehaviour = Always, 
    expire: Duration = null
  ) (implicit format: Format): F[Boolean] = {

    val expireCmd = if (expire != null) {
      List("PX", expire.toMillis.toString)
    } else {
      List.empty
    }
    val cmd = List(key, value) ::: expireCmd ::: whenSet.command
    send("SET", cmd)(asBoolean)
  }
}