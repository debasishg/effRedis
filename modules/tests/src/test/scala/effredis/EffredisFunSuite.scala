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

import cluster.RedisClusterClient
import java.net.URI
import cats.effect._
import Log.NoOp._
import munit.FunSuite
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration.Duration

abstract class EffRedisFunSuite(isCluster: Boolean = false) extends FunSuite {

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val timer: Timer[IO]     = IO.timer(ExecutionContext.global)

  val flushAllFixture = new Fixture[Unit]("FLUSHALL") {
    def apply(): Unit = ()

    override def afterEach(context: AfterEach): Unit =
      Await.result(flushAll(), Duration.Inf)
  }

  override def munitFixtures = List(flushAllFixture)

  override def munitFlakyOK: Boolean = true

  def withAbstractRedis[A](f: RedisClient[IO] => IO[A]): Future[Unit] =
    RedisClient.make[IO](new URI("http://localhost:6379")).use(f).as(assert(true)).unsafeToFuture()

  def withAbstractRedisCluster[A](f: RedisClusterClient[IO] => IO[A]): Future[Unit] =
    RedisClusterClient.make[IO](new URI("http://127.0.0.1:7000")).flatMap(f).as(assert(true)).unsafeToFuture()

  def withAbstractRedis2[A](f: ((RedisClient[IO], RedisClient[IO])) => IO[A]): Future[Unit] = {
    val x = for {
      r1 <- RedisClient.make[IO](new URI("http://localhost:6379"))
      r2 <- RedisClient.make[IO](new URI("http://localhost:6379"))
    } yield (r1, r2)
    x.use(f).as(assert(true)).unsafeToFuture()
  }

  def withRedis[A](f: RedisClient[IO] => IO[A]): Future[Unit] =
    withAbstractRedis[A](f)

  def withRedisCluster[A](f: RedisClusterClient[IO] => IO[A]): Future[Unit] =
    withAbstractRedisCluster[A](f)

  def withRedis2[A](f: ((RedisClient[IO], RedisClient[IO])) => IO[A]): Future[Unit] =
    withAbstractRedis2[A](f)

  private def flushAll(): Future[Unit] =
    if (isCluster) withRedisCluster(_.flushall)
    else withRedis(_.flushall)

}

object EffRedisFunSuite {
  def getBoolean(resp: Resp[Boolean]): Boolean =
    resp match {
      case Value(value) => value == true
      case _            => false
    }

  def getLong(resp: Resp[Option[Long]]): Option[Long] =
    resp match {
      case Value(Some(value)) => Some(value)
      case _                  => None
    }

  def getResp(resp: Resp[_]): Option[_] = resp match {
    case Value(s @ Some(_)) => s
    case Value(None)        => None
    case Error(err)         => Some(err)
    case _                  => None
  }

  def getRespListSize(resp: Resp[_]): Option[Int] = resp match {
    case Value(Some(ls: List[_])) => Some(ls.size)
    case _                        => None
  }

  def getRespList[A](resp: Resp[_]): Option[List[A]] = resp match {
    case Value(Some(ls: List[_])) => Some(ls.asInstanceOf[List[A]])
    case _                        => None
  }
}
