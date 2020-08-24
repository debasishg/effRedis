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
import javax.net.ssl.SSLContext
import scala.concurrent.ExecutionContext
import java.util.concurrent.Executors

import shapeless.HList
import codecs.Format

import cats.effect._
import cats.implicits._

object RedisClient {
  sealed trait SortOrder
  case object ASC extends SortOrder
  case object DESC extends SortOrder

  sealed trait Aggregate
  case object SUM extends Aggregate
  case object MIN extends Aggregate
  case object MAX extends Aggregate

  private[effredis] def extractDatabaseNumber(connectionUri: java.net.URI): Int =
    Option(connectionUri.getPath)
      .map(path =>
        if (path.isEmpty) 0
        else Integer.parseInt(path.tail)
      )
      .getOrElse(0)

  private[effredis] def acquireAndRelease[F[+_]: Concurrent: ContextShift: Log](
      uri: URI,
      blocker: Blocker
  ): Resource[F, RedisClient[F]] = {

    val acquire: F[RedisClient[F]] = {
      F.debug(s"Acquiring client for uri $uri") *>
        blocker.blockOn((new RedisClient[F](uri, blocker)).pure[F])
    }
    val release: RedisClient[F] => F[Unit] = { c =>
      F.debug(s"Releasing client for uri $uri") *> {
        c.close()
        ().pure[F]
      }
    }

    Resource.make(acquire)(release)
  }

  private[effredis] def acquireAndReleaseSequencingDecorator[F[+_]: Concurrent: ContextShift: Log](
      parent: RedisClient[F],
      pipelineMode: Boolean,
      blocker: Blocker
  ): Resource[F, SequencingDecorator[F]] = {

    val acquire: F[SequencingDecorator[F]] = {
      blocker.blockOn((new SequencingDecorator[F](parent, pipelineMode)).pure[F])
    }
    val release: SequencingDecorator[F] => F[Unit] = { c =>
      c.disconnect
      ().pure[F]
    }

    Resource.make(acquire)(release)
  }

  /**
    * This smart constructor is used for `RedisClientPool` to make individual
    * instances
    */
  def build[F[+_]: ContextShift: Concurrent: Log](
      uri: URI
  ): F[RedisClient[F]] = {
    val blocker = Blocker.liftExecutionContext(ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1)))
    (new RedisClient[F](uri, blocker)).pure[F]
  }

  def make[F[+_]: ContextShift: Concurrent: Log](
      uri: URI
  ): Resource[F, RedisClient[F]] =
    for {
      blocker <- RedisBlocker.make()
      client <- acquireAndRelease(uri, blocker)
    } yield client

  private def withSequencingDecorator[F[+_]: ContextShift: Concurrent: Log](
      parent: RedisClient[F],
      pipelineMode: Boolean
  ): Resource[F, SequencingDecorator[F]] =
    for {
      blocker <- RedisBlocker.make()
      client <- acquireAndReleaseSequencingDecorator(parent, pipelineMode, blocker)
    } yield client

  def transact[F[+_]: ContextShift: Concurrent: Log](
      parent: RedisClient[F]
  ): Resource[F, SequencingDecorator[F]] = withSequencingDecorator(parent, false)

  def pipe[F[+_]: ContextShift: Concurrent: Log](
      parent: RedisClient[F]
  ): Resource[F, SequencingDecorator[F]] = withSequencingDecorator(parent, true)

}

class RedisClient[F[+_]: Concurrent: ContextShift: Log](
    override val host: String,
    override val port: Int,
    override val database: Int = 0,
    override val secret: Option[Any] = None,
    override val timeout: Int = 0,
    override val sslContext: Option[SSLContext] = None,
    val blocker: Blocker
) extends RedisCommand[F] {

  def conc: cats.effect.Concurrent[F]  = implicitly[Concurrent[F]]
  def ctx: cats.effect.ContextShift[F] = implicitly[ContextShift[F]]
  def l: Log[F]                        = implicitly[Log[F]]

  def this(b: Blocker) = this("localhost", 6379, blocker = b)
  def this(connectionUri: java.net.URI, b: Blocker) = this(
    host = connectionUri.getHost,
    port = connectionUri.getPort,
    database = RedisClient.extractDatabaseNumber(connectionUri),
    secret = Option(connectionUri.getUserInfo)
      .flatMap(_.split(':') match {
        case Array(_, password, _*) => Some(password)
        case _                      => None
      }),
    blocker = b
  )
  override def toString: String = host + ":" + String.valueOf(port) + "/" + database
  override def close(): Unit    = { disconnect; () }

  def pipeline(client: SequencingDecorator[F])(f: () => Any): F[Resp[Option[List[Any]]]] = {
    implicit val b = blocker
    try {
      val _ = f()
      client.parent
        .send(client.commandBuffer.toString, true)(Some(client.handlers.map(_._2).map(_()).toList))
    } catch {
      case e: Exception => Error(e.getMessage).pure[F]
    }
  }

  def hpipeline[In <: HList](
      client: SequencingDecorator[F]
  )(commands: () => In): F[Resp[Option[List[Any]]]] = {
    implicit val b = blocker
    try {
      val _ = commands()
      client.parent
        .send(client.commandBuffer.toString, true)(Option(client.handlers.map(_._2).map(_()).toList))
        .flatTap { r =>
          client.handlers = Vector.empty
          client.commandBuffer = new StringBuffer
          r.pure[F]
        }
    } catch {
      case e: Exception => Error(e.getMessage).pure[F]
    }
  }

  def htransaction[In <: HList](
      client: SequencingDecorator[F]
  )(commands: () => In): F[Resp[Option[List[Any]]]] =
    multi.flatMap { _ =>
      try {
        val _ = commands()

        if (client.handlers
              .map(_._1)
              .filter(_ == "DISCARD")
              .isEmpty) {

          // exec only if no discard
          F.debug(s"Executing transaction ..") >> {
            try {
              exec(client.handlers.map(_._2)).flatTap { _ =>
                client.handlers = Vector.empty
                ().pure[F]
              }
            } catch {
              case e: Exception =>
                Error(e.getMessage()).pure[F]
            }
          }

        } else {
          // no exec if discard
          F.debug(s"Got DISCARD .. discarding transaction") >> {
            TxnDiscarded(client.handlers).pure[F].flatTap { r =>
              client.handlers = Vector.empty
              r.pure[F]
            }
          }
        }
      } catch {
        case e: Exception =>
          Error(e.getMessage()).pure[F]
      }
    }

  def transaction(
      client: SequencingDecorator[F]
  )(f: () => Any): F[Resp[Option[List[Any]]]] = {

    implicit val b = blocker

    send("MULTI")(asString).flatMap { _ =>
      try {
        val _ = f()

        // no exec if discard
        if (client.handlers
              .map(_._1)
              .filter(_ == "DISCARD")
              .isEmpty) {

          send("EXEC")(asExec(client.handlers.map(_._2))).flatTap { _ =>
            client.handlers = Vector.empty
            ().pure[F]
          }
        } else {
          TxnDiscarded(client.handlers).pure[F].flatTap { r =>
            client.handlers = Vector.empty
            r.pure[F]
          }
        }

      } catch {
        case e: Exception =>
          Error(e.getMessage()).pure[F]
      }
    }
  }
}

class SequencingDecorator[F[+_]: Concurrent: ContextShift: Log](
    val parent: RedisClient[F],
    pipelineMode: Boolean = false
) extends RedisCommand[F] {

  def conc: cats.effect.Concurrent[F]  = implicitly[Concurrent[F]]
  def ctx: cats.effect.ContextShift[F] = implicitly[ContextShift[F]]
  def blocker: Blocker                 = parent.blocker
  def l: Log[F]                        = implicitly[Log[F]]

  var handlers: Vector[(String, () => Any)] = Vector.empty
  var commandBuffer: StringBuffer           = new StringBuffer

  override def send[A](command: String, args: Seq[Any])(
      result: => A
  )(implicit format: Format, blocker: Blocker): F[Resp[A]] = blocker.blockOn {
    try {
      val cmd  = Commands.multiBulk(command.getBytes("UTF-8") +: (args map (format.apply)))
      val crlf = "\r\n"
      if (!pipelineMode) { // transaction mode
        write(cmd)
        handlers :+= ((command, () => result))
        val _ = receive(singleLineReply)
        Queued.pure[F]

      } else { // pipeline mode
        handlers :+= ((command, () => result))
        commandBuffer.append((List(command) ++ args.toList).mkString(" ") ++ crlf)
        Buffered.pure[F]
      }
    } catch {
      case e: Exception => Error(e.getMessage()).pure[F]
    }
  }

  override def send[A](command: String, pipeline: Boolean = false)(
      result: => A
  )(implicit blocker: Blocker): F[Resp[A]] =
    blocker.blockOn {
      try {
        val cmd  = Commands.multiBulk(List(command.getBytes("UTF-8")))
        val crlf = "\r\n"
        if (!pipelineMode) {
          write(cmd)
          handlers :+= ((command, () => result))
          val _ = receive(singleLineReply)
          Queued.pure[F]
        } else {
          handlers :+= ((command, () => result))
          commandBuffer.append(command ++ crlf)
          Buffered.pure[F]
        }
      } catch {
        case e: Exception => Error(e.getMessage()).pure[F]
      }
    }

  val host              = parent.host
  val port              = parent.port
  val timeout           = parent.timeout
  override val secret   = parent.secret
  override val database = parent.database

  // TODO: Find a better abstraction
  override def connected                = parent.connected
  override def connect                  = parent.connect
  override def disconnect               = parent.disconnect
  override def clearFd()                = parent.clearFd()
  override def write(data: Array[Byte]) = parent.write(data)
  override def readLine                 = parent.readLine
  override def readCounted(count: Int)  = parent.readCounted(count)
  override def onConnect()              = parent.onConnect()

  override def close(): Unit = parent.close()
}
