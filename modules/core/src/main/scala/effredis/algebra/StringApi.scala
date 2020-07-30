package effredis.algebra

import effredis.serialization.Format  // {Format, Parse}

import scala.concurrent.duration.Duration

trait StringApi[F[_]] {
  import StringApi._

  /**
    * sets the key with the specified value.
    * Starting with Redis 2.6.12 SET supports a set of options that modify its behavior:
    *
    * NX -- Only set the key if it does not already exist.
    * XX -- Only set the key if it already exist.
    * PX milliseconds -- Set the specified expire time, in milliseconds.
    */
  def set(key: Any, value: Any, whenSet: SetBehaviour = Always, expire: Duration = null)
         (implicit format: Format): F[Boolean]

}

object StringApi {

  sealed abstract class SetBehaviour(val command: List[String]) // singleton list
  case object NX     extends SetBehaviour(List("NX"))
  case object XX     extends SetBehaviour(List("XX"))
  case object Always extends SetBehaviour(List.empty)

}