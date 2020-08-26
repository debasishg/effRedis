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

import java.net.SocketException

import cats.effect._
import cats.implicits._

import codecs.Format

abstract class Redis[F[+_]: Concurrent: ContextShift: Log] extends RedisIO with Protocol {

  def send[A](command: String, args: Seq[Any])(
      result: => A
  )(implicit format: Format, blocker: Blocker): F[Resp[A]] = {

    val cmd = Commands.multiBulk(command.getBytes("UTF-8") +: (args map (format.apply)))
    F.debug(s"Sending ${new String(cmd)}") >> {
      try {

        write(cmd)
        Value(result).pure[F]

      } catch {
        case e: RedisConnectionException =>
          if (disconnect) send(command, args)(result)
          else Error(e.getMessage()).pure[F]
        case e: SocketException =>
          if (disconnect) send(command, args)(result)
          else Error(e.getMessage()).pure[F]
        case e: Exception =>
          Error(e.getMessage()).pure[F]
      }
    }
  }

  def send[A](command: String, pipelineMode: Boolean = false)(
      result: => A
  )(implicit blocker: Blocker): F[Resp[A]] = {
    val cmd = Commands.multiBulk(List(command.getBytes("UTF-8")))
    F.debug(s"Sending ${new String(cmd)}") >> {

      try {

        if (!pipelineMode) write(cmd)
        else write(command.getBytes("UTF-8"))
        Value(result).pure[F]

      } catch {
        case e: RedisConnectionException =>
          if (disconnect) send(command)(result)
          else Error(e.getMessage()).pure[F]
        case e: SocketException =>
          if (disconnect) send(command)(result)
          else Error(e.getMessage()).pure[F]
        case e: Exception =>
          Error(e.getMessage()).pure[F]
      }
    }
  }

  def cmd(args: Seq[Array[Byte]]): Array[Byte] = Commands.multiBulk(args)

  protected def flattenPairs(in: Iterable[Product2[Any, Any]]): List[Any] =
    in.iterator.flatMap(x => Iterator(x._1, x._2)).toList
}
