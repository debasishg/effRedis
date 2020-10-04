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

package effredis.props

import java.net.URI
import cats.effect._
import org.scalacheck._
import org.scalacheck.effect.PropF
import effredis._
import effredis.Log.NoOp._

import EffRedisPropsFunSuite._

class ListSuite extends EffRedisPropsFunSuite {
  // generate values for lists
  implicit lazy val genValue: Gen[String] =
    Gen
      .listOfN(
        6,
        Gen.alphaChar
      )
      .suchThat(_.size >= 4)
      .map(_.mkString)

  test("redis list operations lpush and rpush increases the length of the list by the number of elements pushed") {
    RedisClient
      .single[IO](new URI("http://localhost:6379"))
      .use { cl =>
        import cl._
        IO {
          PropF.forAllF(Gen.listOfN(10, genValue)) { (v: List[String]) =>
            for {
              y <- llen("list-1")
              z <- llen("list-2")
              x <- if (v.size <= 1) {
                    lpush("list-1", v.head)
                      .map(res => assert(lengthIncreasesBy(y, res, 1)))
                    rpush("list-2", v.head)
                      .map(res => assert(lengthIncreasesBy(z, res, 1)))
                  } else {
                    lpush("list-1", v.head, v.tail: _*)
                      .map(res => assert(lengthIncreasesBy(y, res, v.tail.size + 1)))
                    rpush("list-2", v.head, v.tail: _*)
                      .map(res => assert(lengthIncreasesBy(z, res, v.tail.size + 1)))
                  }
            } yield x
          }
        }
      }
      .unsafeRunSync()
  }

  test("redis list operations lpush / rpush followed by lpop / rpop / ltrim truncates the list") {
    RedisClient
      .single[IO](new URI("http://localhost:6379"))
      .use { cl =>
        import cl._
        IO {
          PropF.forAllF(Gen.listOfN(10, genValue).suchThat(_.size > 0)) { (v: List[String]) =>
            for {
              x <- if (v.size <= 1) {
                    (lpush("list-1", v.head) *> lpop("list-1") *>
                        llen("list-1")).map(r => assert(getLong(r).get == 0))
                    (rpush("list-1", v.head) *> rpop("list-1") *>
                        llen("list-1")).map(r => assert(getLong(r).get == 0))
                  } else {
                    (lpush("list-1", v.head, v.tail: _*) *>
                        ltrim("list-1", 2, 1) *> llen("list-1")).map(r => assert(getLong(r).get == 0))
                    (rpush("list-1", v.head, v.tail: _*) *>
                        ltrim("list-1", 2, 1) *> llen("list-1")).map(r => assert(getLong(r).get == 0))
                  }
            } yield x
          }
        }
      }
      .unsafeRunSync()
  }

  test("test redis list operations lrange should return the full list pushed using lpush or rpush") {
    RedisClient
      .single[IO](new URI("http://localhost:6379"))
      .use { cl =>
        import cl._
        IO {
          PropF.forAllF(Gen.listOfN(10, genValue).suchThat(_.size > 1)) { (v: List[String]) =>
            for {
              _ <- (lpush("list-1", v.head, v.tail: _*) *> lrange("list-1", 0, -1))
                     .map(r => assert(getRespListSize(r).get == v.length))
              _ <- ltrim("list-1", 2, 1)
              _ <- (rpush("list-1", v.head, v.tail: _*) *> lrange("list-1", 0, -1))
                     .map(r => assert(getRespListSize(r).get == v.length))
              _ <- ltrim("list-1", 2, 1)
            } yield ()
          }
        }
      }
      .unsafeRunSync()
  }

  test("test redis list operations lrange") {
    RedisClient
      .single[IO](new URI("http://localhost:6379"))
      .use { cl =>
        import cl._
        IO {
          PropF.forAllF(Gen.listOfN(10, genValue)) { (v: List[String]) =>
            for {
              y <- llen("list-1")
              _ <- lpush("list-1", v.head, v.tail: _*).map(r =>
                    assert(getLong(r).get == getLong(y).get + v.tail.size + 1)
                  )
              z <- llen("list-1")
              _ <- lrange("list-1", 0, getLong(z).get.toInt).map(r => getRespListSize(r).get == getLong(z).get)
              _ <- lrange("list-1", 0, 0).map(r => getRespListSize(r).get == 0)
              _ <- ltrim("list-1", 1, -1)
              _ <- llen("list-1").map(r => getLong(r).get == getLong(z).get.toInt - 1)
              _ <- ltrim("list-1", 0, getLong(z).get.toInt)
              _ <- llen("list-1").map(r => getLong(r).get == 0)
            } yield ()
          }
        }
      }
      .unsafeRunSync()
  }
}
