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

// examples from Favio Labella's talk at Scala Italy 2019
package effredis.cluster
package util

import cats.effect._
import cats.syntax.all._
import cats.effect.{ Deferred, Ref }

abstract class Cached[F[_], A] {
  def get: F[A]
  def expire: F[Unit]
}

object Cached {
  def create[F[_]: Concurrent, A](fa: F[A]): F[Cached[F, A]] = {
    sealed trait State
    case class Value(v: A) extends State

    // waiting involved for the action to finish
    // since we have waiting, we model using Deferred
    // since F[A] can fail we have Either[Throwable, A]
    case class Updating(d: Deferred[F, Either[Throwable, A]]) extends State

    // either no one has called get or someone has called expire and after that
    // no one has called get
    case object NoValue extends State

    Ref.of[F, State](NoValue).map { state =>
      new Cached[F, A] {
        // memoized : the first time you call get the action gets executed
        // from next invocation onwards it's just fetch
        def get: F[A] =
          Deferred[F, Either[Throwable, A]].flatMap { newV =>
            state.modify {
              // return the value in the context of F
              case st @ Value(v) => st -> v.pure[F]

              // wait in Updating state, we will not compute again
              case st @ Updating(inFlight) => st -> inFlight.get.rethrow

              // move to state Updating and call fetch
              // fetch starts the task and puts the result in the proper place
              case NoValue => Updating(newV) -> fetch(newV).rethrow
            }.flatten
          }

        def fetch(d: Deferred[F, Either[Throwable, A]]) =
          for {
            r <- fa.attempt
            _ <- state.set {
                  r match {
                    case Left(_)  => NoValue
                    case Right(v) => Value(v)
                  }
                }
            _ <- d.complete(r)
          } yield r

        def expire: F[Unit] = state.update {
          case Value(_)         => NoValue
          case NoValue          => NoValue
          case st @ Updating(_) => st
        }
      }
    }
  }
}

// object Main extends IOApp {
//
//   import scala.concurrent.duration._
//   import java.time.LocalDateTime
//
//   def run(args: List[String]): cats.effect.IO[cats.effect.ExitCode] = {
//     // create
//     val program = IO { println(s"Hey! ${LocalDateTime.now()}"); 42 }
//     val cio     = Cached.create[IO, Int](program)
//     val c       = cio.unsafeRunSync()
//
//     // schedule expiry every 10 seconds
//
//     val getJob = for {
//       v <- c.get
//       _ <- IO(println(v))
//     } yield ()
//     import ClusterUtils._
//
//     (for {
//       _ <- repeatAtFixedRate(10.seconds, c.expire).start //.start.unsafeRunSync()
//       _ <- repeatAtFixedRate(3.seconds, getJob) // .unsafeRunSync()
//     } yield ()).unsafeRunSync
//
//     IO(ExitCode.Success)
//
//   }
// //     val program = IO { println("Hey!"); 42 }
// //     for {
// //       c <- Cached.create[IO, Int](program)
// //       x <- c.get
// //       y <- c.get
// //       _ <- c.expire
// //       z <- c.get
// //       _ <- IO(println(s"$x, $y, $z"))
// //     } yield (ExitCode.Success)
// //   }
// }
//
