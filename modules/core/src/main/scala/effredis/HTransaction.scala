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

package effredis

import java.net.URI
import util.hlist._

import cats.effect._
// import cats.implicits._

object HTransaction extends IOApp {
  override def run(args: List[String]): IO[ExitCode] =
    RedisClient.make[IO](new URI("http://localhost:6379")).use { cli =>
      RedisClient.withSequencingDecorator[IO](cli).use { txnClient =>
        import txnClient._

        // val cmds = txnClient.parent.multi :: set("k1", "v1") :: set("k2", "v2") :: get("k1") :: get("k2") :: HNil
        // val r = cli.htxn1(txnClient)(cmds)

        val cmds = { () => 
          set("k1", "v1") :: 
          set("k2", "v2") :: 
          get("k1")       :: 
          get("k2")       :: 
          HNil 
        }

        val r = cli.htxn2(txnClient)(cmds)

        r.unsafeRunSync() match {

          case Right(Right(Right(Some(ls)))) => { println("in success"); ls.foreach(println) }
          // case Some(Right(Right(Right(Some(ls))))) => { println("in success"); println(ls.unsafeRunSync) }
//           case Left(state) =>
//             state match {
//               case TxnDiscarded      => println("Transaction discarded")
//               case TxnError(message) => println(message)
//             }
          case err => println(s"oops! $err")
        }
        IO(ExitCode.Success)
      }
    }
}
