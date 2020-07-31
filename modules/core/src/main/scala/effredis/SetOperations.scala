package effredis

import cats.effect._
import algebra.SetApi
import serialization._

trait SetOperations[F[_]] extends SetApi[F] { self: Redis =>
  implicit def blocker: Blocker
  implicit def conc: Concurrent[F]
  implicit def ctx: ContextShift[F]

  override def sadd(key: Any, value: Any, values: Any*)(implicit format: Format): F[Option[Long]] =
    send("SADD", List(key, value) ::: values.toList)(asLong)

  override def srem(key: Any, value: Any, values: Any*)(implicit format: Format): F[Option[Long]] =
    send("SREM", List(key, value) ::: values.toList)(asLong)

  override def spop[A](key: Any)(implicit format: Format, parse: Parse[A]): F[Option[A]] =
    send("SPOP", List(key))(asBulk)

  override def spop[A](key: Any, count: Int)(implicit format: Format, parse: Parse[A]): F[Option[Set[Option[A]]]] =
    send("SPOP", List(key, count))(asSet)

  override def smove(sourceKey: Any, destKey: Any, value: Any)(implicit format: Format): F[Option[Long]] =
    send("SMOVE", List(sourceKey, destKey, value))(asLong)

  override def scard(key: Any)(implicit format: Format): F[Option[Long]] =
    send("SCARD", List(key))(asLong)

  override def sismember(key: Any, value: Any)(implicit format: Format): F[Boolean] =
    send("SISMEMBER", List(key, value))(asBoolean)

  override def sinter[A](key: Any, keys: Any*)(implicit format: Format, parse: Parse[A]): F[Option[Set[Option[A]]]] =
    send("SINTER", key :: keys.toList)(asSet)

  override def sinterstore(key: Any, keys: Any*)(implicit format: Format): F[Option[Long]] =
    send("SINTERSTORE", key :: keys.toList)(asLong)

  override def sunion[A](key: Any, keys: Any*)(implicit format: Format, parse: Parse[A]): F[Option[Set[Option[A]]]] =
    send("SUNION", key :: keys.toList)(asSet)

  override def sunionstore(key: Any, keys: Any*)(implicit format: Format): F[Option[Long]] =
    send("SUNIONSTORE", key :: keys.toList)(asLong)

  override def sdiff[A](key: Any, keys: Any*)(implicit format: Format, parse: Parse[A]): F[Option[Set[Option[A]]]] =
    send("SDIFF", key :: keys.toList)(asSet)

  override def sdiffstore(key: Any, keys: Any*)(implicit format: Format): F[Option[Long]] =
    send("SDIFFSTORE", key :: keys.toList)(asLong)

  override def smembers[A](key: Any)(implicit format: Format, parse: Parse[A]): F[Option[Set[Option[A]]]] =
    send("SMEMBERS", List(key))(asSet)

  override def srandmember[A](key: Any)(implicit format: Format, parse: Parse[A]): F[Option[A] ]=
    send("SRANDMEMBER", List(key))(asBulk)

  override def srandmember[A](key: Any, count: Int)(implicit format: Format, parse: Parse[A]): F[Option[List[Option[A]]]] =
    send("SRANDMEMBER", List(key, count))(asList)

  override def sscan[A](key: Any, cursor: Int, pattern: Any = "*", count: Int = 10)(implicit format: Format, parse: Parse[A]): F[Option[(Option[Int], Option[List[Option[A]]])]] =
    send("SSCAN", key :: cursor :: ((x: List[Any]) => if (pattern == "*") x else "match" :: pattern :: x) (if (count == 10) Nil else List("count", count)))(asPair)
}