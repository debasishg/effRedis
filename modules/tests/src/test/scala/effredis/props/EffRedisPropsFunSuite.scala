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

package effredis.props

import java.net.URI
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration.Duration
import cats.effect._
import munit.CatsEffectSuite
import munit.ScalaCheckEffectSuite
import effredis.RedisClient
import effredis.Log.NoOp._
import effredis._

abstract class EffRedisPropsFunSuite extends CatsEffectSuite with ScalaCheckEffectSuite {

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val tr: Timer[IO]        = IO.timer(ExecutionContext.global)

  val flushAllFixture = new Fixture[Unit]("FLUSHALL") {
    def apply(): Unit = ()

    override def afterEach(context: AfterEach): Unit =
      Await.result(flushAll(), Duration.Inf)
  }

  override def munitFixtures = List(flushAllFixture)

  override def munitFlakyOK: Boolean = true

  final def withAbstractRedis[A](f: RedisClient[IO, RedisClient.SINGLE.type] => IO[A]): Future[Unit] =
    RedisClient.single[IO](new URI("http://localhost:6379")).use(f).as(assert(true)).unsafeToFuture()

  final def withRedis[A](f: RedisClient[IO, RedisClient.SINGLE.type] => IO[A]): Future[Unit] =
    withAbstractRedis[A](f)

  private def flushAll(): Future[Unit] =
    withRedis(_.flushall)
}

object EffRedisPropsFunSuite {
  final def getBoolean(resp: Resp[Boolean]): Boolean =
    resp match {
      case Value(value) => value == true
      case _            => false
    }

  final def getLong(resp: Resp[Option[Long]]): Option[Long] =
    resp match {
      case Value(Some(value)) => Some(value)
      case _                  => None
    }

  final def getResp(resp: Resp[_]): Option[_] = resp match {
    case Value(s @ Some(_)) => s
    case Value(None)        => None
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
