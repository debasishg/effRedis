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

import java.net.{ SocketException, URI }
import javax.net.ssl.SSLContext

import util.hlist._
import codecs.{ Format, Parse }

import cats.effect._
import cats.implicits._

sealed trait TransactionState
case class TxnDiscarded(contents: Vector[(String, () => Any)]) extends TransactionState
case class TxnError(message: String) extends TransactionState

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
      blocker.blockOn((new RedisClient[F](uri, blocker)).pure[F])
    }
    val release: RedisClient[F] => F[Unit] = { c =>
      c.disconnect
      ().pure[F]
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

  def make[F[+_]: ContextShift: Concurrent: Log](
      uri: URI
  ): Resource[F, RedisClient[F]] =
    for {
      blocker <- RedisBlocker.make
      client <- acquireAndRelease(uri, blocker)
    } yield client

  def withSequencingDecorator[F[+_]: ContextShift: Concurrent: Log](
      parent: RedisClient[F],
      pipelineMode: Boolean = false
  ): Resource[F, SequencingDecorator[F]] =
    for {
      blocker <- RedisBlocker.make
      client <- acquireAndReleaseSequencingDecorator(parent, pipelineMode, blocker)
    } yield client
}

abstract class Redis[F[+_]: Concurrent: ContextShift: Log] extends RedisIO with Protocol {

  def send[A](command: String, args: Seq[Any])(
      result: => A
  )(implicit format: Format, blocker: Blocker): F[RedisResponse[A]] = blocker.blockOn {

    val cmd = Commands.multiBulk(command.getBytes("UTF-8") +: (args map (format.apply)))
    F.debug(s"Sending ${new String(cmd)}") >> {
      try {

        write(cmd)
        Right(result).pure[F]

      } catch {
        case e: RedisConnectionException =>
          if (disconnect) send(command, args)(result)
          else Left(Left(e.getMessage())).pure[F]
        case e: SocketException =>
          if (disconnect) send(command, args)(result)
          else Left(Left(e.getMessage())).pure[F]
        case e: Exception =>
          Left(Left(e.getMessage())).pure[F]
      }
    }
  }

  def send[A](command: String, pipelineMode: Boolean = false)(
      result: => A
  )(implicit blocker: Blocker): F[RedisResponse[A]] =
    blocker.blockOn {
      val cmd = Commands.multiBulk(List(command.getBytes("UTF-8")))
      F.debug(s"Sending ${new String(cmd)}") >> {

        try {

          if (!pipelineMode) write(cmd)
          else write(command.getBytes("UTF-8"))
          Right(result).pure[F]

        } catch {
          case e: RedisConnectionException =>
            if (disconnect) send(command)(result)
            else Left(Left(e.getMessage())).pure[F]
          case e: SocketException =>
            if (disconnect) send(command)(result)
            else Left(Left(e.getMessage())).pure[F]
          case e: Exception =>
            Left(Left(e.getMessage())).pure[F]
        }
      }
    }

  def cmd(args: Seq[Array[Byte]]): Array[Byte] = Commands.multiBulk(args)

  protected def flattenPairs(in: Iterable[Product2[Any, Any]]): List[Any] =
    in.iterator.flatMap(x => Iterator(x._1, x._2)).toList
}

trait RedisCommand[F[+_]]
    extends Redis[F]
    with StringOperations[F]
    with BaseOperations[F]
    with ListOperations[F]
    with SetOperations[F]
    with HashOperations[F]
    with SortedSetOperations[F]
    with NodeOperations[F]
    with GeoOperations[F]
    with EvalOperations[F]
    with HyperLogLogOperations[F]
//     with PubOperations
    with AutoCloseable {

  val database: Int       = 0
  val secret: Option[Any] = None

  override def onConnect(): Unit = {
    secret.foreach(s => auth(s))
    selectDatabase()
  }

  private def selectDatabase(): Unit = {
    val _ = if (database != 0) select(database)
    ()
  }
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

  def pipeline(client: SequencingDecorator[F])(f: () => Any): F[RedisResponse[Option[List[Any]]]] = {
    implicit val b = blocker
    try {
      val _ = f()
      client.parent
        .send(client.commandBuffer.toString, true)(Some(client.handlers.map(_._2).map(_()).toList))
    } catch {
      case e: Exception => Left(Left(e.getMessage)).pure[F]
    }
  }

  def hpipeline[In <: HList](
      client: SequencingDecorator[F]
  )(commands: () => In): F[RedisResponse[Option[List[Any]]]] = {
    implicit val b = blocker
    try {
      val _ = commands()
      client.parent
        .send(client.commandBuffer.toString, true)(Some(client.handlers.map(_._2).map(_()).toList)).flatTap {r =>
          client.handlers = Vector.empty
          client.commandBuffer = new StringBuffer
          r.pure[F]
        }
    } catch {
      case e: Exception => Left(Left(e.getMessage)).pure[F]
    }
  }

  def htransaction[In <: HList](
      client: SequencingDecorator[F]
  )(commands: () => In): F[Either[TransactionState, RedisResponse[Option[List[Any]]]]] =
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
              exec(client.handlers.map(_._2)).map(Right(_)).flatTap { _ =>
                client.handlers = Vector.empty
                ().pure[F]
              }
            } catch {
              case e: Exception =>
                Left(TxnError(e.getMessage())).pure[F]
            }
          }

        } else {
          // no exec if discard
          F.debug(s"Got DISCARD .. discarding transaction") >> {
            Left(TxnDiscarded(client.handlers)).pure[F].flatTap {r =>
              client.handlers = Vector.empty
              r.pure[F]
            }
          }
        }
      } catch {
        case e: Exception =>
          Left(TxnError(e.getMessage())).pure[F]
      }
    }

  def transaction(
      client: SequencingDecorator[F]
  )(f: () => Any): F[Either[TransactionState, RedisResponse[Option[List[Any]]]]] = {

    implicit val b = blocker

    send("MULTI")(asString).flatMap { _ =>
      try {
        val _ = f()

        // no exec if discard
        if (client.handlers
              .map(_._1)
              .filter(_ == "DISCARD")
              .isEmpty) {

          send("EXEC")(asExec(client.handlers.map(_._2))).map(Right(_)).flatTap { _ =>
            client.handlers = Vector.empty
            ().pure[F]
          }
        } else {
          Left(TxnDiscarded(client.handlers)).pure[F].flatTap {r =>
            client.handlers = Vector.empty
            r.pure[F]
          }
        }

      } catch {
        case e: Exception =>
          Left(TxnError(e.getMessage())).pure[F]
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

  var handlers: Vector[(String, () => Any)] = Vector.empty
  var commandBuffer: StringBuffer           = new StringBuffer

  override def send[A](command: String, args: Seq[Any])(
      result: => A
  )(implicit format: Format, blocker: Blocker): F[RedisResponse[A]] = blocker.blockOn {
    try {
      val cmd  = Commands.multiBulk(command.getBytes("UTF-8") +: (args map (format.apply)))
      val crlf = "\r\n"
      if (!pipelineMode) { // transaction mode
        write(cmd)
        handlers :+= ((command, () => result))
        Left(Right(receive(singleLineReply).map(Parse.parseDefault))).pure[F]

      } else { // pipeline mode
        handlers :+= ((command, () => result))
        commandBuffer.append((List(command) ++ args.toList).mkString(" ") ++ crlf)
        Left(Right(Some("Buffered"))).pure[F]
      }
    } catch {
      case e: Exception => Left(Left(e.getMessage())).pure[F]
    }
  }

  override def send[A](command: String, pipeline: Boolean = false)(
      result: => A
  )(implicit blocker: Blocker): F[RedisResponse[A]] =
    blocker.blockOn {
      try {
        val cmd  = Commands.multiBulk(List(command.getBytes("UTF-8")))
        val crlf = "\r\n"
        if (!pipelineMode) {
          write(cmd)
          handlers :+= ((command, () => result))
          Left(Right(receive(singleLineReply).map(Parse.parseDefault))).pure[F]
        } else {
          handlers :+= ((command, () => result))
          commandBuffer.append(command ++ crlf)
          Left(Right(Some("Buffered"))).pure[F]
        }
      } catch {
        case e: Exception => Left(Left(e.getMessage())).pure[F]
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
