package effredis

import cats.effect._
import algebra.HyperLogLogApi

trait HyperLogLogOperations[F[_]] extends HyperLogLogApi[F] { self: Redis =>
  implicit def blocker: Blocker
  implicit def conc: Concurrent[F]
  implicit def ctx: ContextShift[F]

  override def pfadd(key: Any, value: Any, values: Any*): F[Option[Long]] =
    send("PFADD", List(key, value) ::: values.toList)(asLong)

  override def pfcount(keys: Any*): F[Option[Long]] =
    send("PFCOUNT", keys.toList)(asLong)

  override def pfmerge(destination: Any, sources: Any*): F[Boolean] =
    send("PFMERGE", List(destination) ::: sources.toList)(asBoolean)
}