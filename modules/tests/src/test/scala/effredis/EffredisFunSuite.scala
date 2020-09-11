/*
 * Copyright 2020 Debasish Ghosh
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// adopted from :
// https://github.com/profunktor/redis4cats/blob/master/modules/tests/src/test/scala/dev/profunktor/redis4cats/Redis4CatsFunSuite.scala

package effredis

import java.net.URI
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration._
import cats.data.NonEmptyList
import cats.effect._
import Log.NoOp._
import munit.FunSuite
import org.scalacheck.effect.PropF
import RedisClient._
import cluster.RedisClusterClient

abstract class EffRedisFunSuite(isCluster: Boolean = false) extends FunSuite {

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val tr: Timer[IO]        = IO.timer(ExecutionContext.global)

  val flushAllFixture = new Fixture[Unit]("FLUSHALL") {
    def apply(): Unit = ()

    override def afterEach(context: AfterEach): Unit =
      if (isCluster) {
        flushAllCluster()
        ()
      } else Await.result(flushAll(), Duration.Inf)
  }

  override def munitFixtures = List(flushAllFixture)

  override def munitFlakyOK: Boolean = true

  final def withAbstractRedis[A](f: RedisClient[IO, RedisClient.SINGLE.type] => IO[A]): Future[Unit] =
    RedisClient.single[IO](new URI("http://localhost:6379")).use(f).as(assert(true)).unsafeToFuture()

  final def withAbstractRedisForURI[A](uri: URI)(f: RedisClient[IO, RedisClient.SINGLE.type] => IO[A]): Future[Unit] =
    RedisClient.single[IO](uri).use(f).as(assert(true)).unsafeToFuture()

  final def withAbstractRedisCluster[A](f: RedisClusterClient[IO, SINGLE.type] => IO[A]): IO[Unit] =
    RedisClusterClient
      .make[IO, SINGLE.type](NonEmptyList.one(new URI("http://127.0.0.1:7000")))
      .flatMap(f)
      .as(assert(true))

  final def withAbstractRedis2[A](
      f: ((RedisClient[IO, RedisClient.SINGLE.type], RedisClient[IO, RedisClient.SINGLE.type])) => IO[A]
  ): Future[Unit] = {
    val x = for {
      r1 <- RedisClient.single[IO](new URI("http://localhost:6379"))
      r2 <- RedisClient.single[IO](new URI("http://localhost:6379"))
    } yield (r1, r2)
    x.use(f).as(assert(true)).unsafeToFuture()
  }

  final def withEffectfulAbstractRedis[A](f: RedisClient[IO, RedisClient.SINGLE.type] => IO[PropF[IO]]): Future[Unit] =
    RedisClient.single[IO](new URI("http://localhost:6379")).use(f).as(assert(true)).unsafeToFuture()

  final def withRedis[A](f: RedisClient[IO, RedisClient.SINGLE.type] => IO[A]): Future[Unit] =
    withAbstractRedis[A](f)

  final def withRedisForURI[A](uri: URI)(f: RedisClient[IO, RedisClient.SINGLE.type] => IO[A]): Future[Unit] =
    withAbstractRedisForURI[A](uri)(f)

  final def withRedisCluster[A](f: RedisClusterClient[IO, SINGLE.type] => IO[A]): IO[Unit] =
    withAbstractRedisCluster[A](f)

  final def withRedis2[A](
      f: ((RedisClient[IO, RedisClient.SINGLE.type], RedisClient[IO, RedisClient.SINGLE.type])) => IO[A]
  ): Future[Unit] =
    withAbstractRedis2[A](f)

  final def withEffectfulRedis[A](f: RedisClient[IO, RedisClient.SINGLE.type] => IO[PropF[IO]]): Future[Unit] =
    withEffectfulAbstractRedis[A](f)

  private def flushAllCluster(): IO[Unit] = IO(())
  /*
    RedisClientPool.poolResource[IO, SINGLE.type](SINGLE).use[IO, Unit] { pool =>
      implicit val p = pool
      withRedisCluster(_.flushall)
    }
   */

  private def flushAll(): Future[Unit] =
    withRedis(_.flushall)
}

object EffRedisFunSuite {
  final def getBoolean(resp: Resp[_]): Boolean =
    resp match {
      case Value("OK") => true
      case Value(true) => true
      case _           => false
    }

  final def getLong(resp: Resp[Option[Long]]): Option[Long] =
    resp match {
      case Value(Some(value)) => Some(value)
      case _                  => None
    }

  final def getResp(resp: Resp[_]): Option[_] = resp match {
    case Value(s @ Some(_)) => s
    case Value(None)        => None
    case Value(v)           => Some(v)
    case Error(err)         => Some(err)
    case _                  => None
  }

  final def getRespListSize(resp: Resp[_]): Option[Int] = resp match {
    case Value(Some(ls: List[_])) => Some(ls.size)
    case _                        => None
  }

  final def getRespList[A](resp: Resp[_]): Option[List[A]] = resp match {
    case Value(Some(ls: List[_])) => Some(ls.asInstanceOf[List[A]])
    case _                        => None
  }
}
