package effredis

import java.net.{ SocketException, URI }
import javax.net.ssl.SSLContext

import serialization.Format

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

  private def extractDatabaseNumber(connectionUri: java.net.URI): Int =
    Option(connectionUri.getPath)
      .map(path =>
        if (path.isEmpty) 0
        else Integer.parseInt(path.tail)
      )
      .getOrElse(0)

  private [effredis] def acquireAndRelease[F[_]: Concurrent: ContextShift](
    uri: URI,
    blocker: Blocker
  ): Resource[F, RedisClient] = {

    val acquire: F[RedisClient] = blocker.blockOn((new RedisClient(uri)).pure[F])
    val release: RedisClient => F[Unit] = c => { c.disconnect; ().pure[F] }
    Resource.make(acquire)(release)
  }

  def makeWithURI[F[_]: ContextShift: Concurrent](
    uri: URI
  ): Resource[F, RedisCommands[F]] = for {
    blocker <- RedisBlocker.make
    client <- acquireAndRelease(uri, blocker)
  } yield {
    implicit val bl: Blocker = blocker 
    RedisCommands(client, 
      new StringOperations[F],
      new BaseOperations[F]) 
  }
}

trait Redis extends RedisIO with Protocol {

  def send[F[_]: Concurrent: ContextShift, A]
    (command: String, args: Seq[Any])
    (result: => A)
    (implicit format: Format, blocker: Blocker): F[A] = blocker.blockOn {

      try {
        write(Commands.multiBulk(command.getBytes("UTF-8") +: (args map (format.apply))))
        result.pure[F]
      } catch {
        case e: RedisConnectionException =>
          if (disconnect) send(command, args)(result)
          else throw e
        case e: SocketException =>
          if (disconnect) send(command, args)(result)
          else throw e
      }
    }

  def send[F[_]: Concurrent: ContextShift, A]
    (command: String)
    (result: => A)
    (implicit blocker: Blocker): F[A] = blocker.blockOn {
    try {
      write(Commands.multiBulk(List(command.getBytes("UTF-8"))))
      result.pure[F]
    } catch {
      case e: RedisConnectionException =>
        if (disconnect) send(command)(result)
        else throw e
      case e: SocketException =>
        if (disconnect) send(command)(result)
        else throw e
    }
  }

  def cmd(args: Seq[Array[Byte]]): Array[Byte] = Commands.multiBulk(args)

  protected def flattenPairs(in: Iterable[Product2[Any, Any]]): List[Any] =
    in.iterator.flatMap(x => Iterator(x._1, x._2)).toList

}

case class RedisCommands[F[_]: ContextShift: Concurrent](
  cli: RedisClient,
  str: algebra.StringApi[F],
  bse: algebra.BaseApi[F]
)

trait RedisCommand
    // extends StringOperations[IO]
    extends Redis
    // with StringApi[IO]
//     with BaseOperations
//     with GeoOperations
//     with NodeOperations
//     with ListOperations
//     with SetOperations
//     with SortedSetOperations
//     with HashOperations
//     with EvalOperations
//     with PubOperations
//     with HyperLogLogOperations
    with AutoCloseable {

  // val database: Int       = 0
  // val secret: Option[Any] = None

  override def onConnect: Unit = ??? // {
//     secret.foreach(s => auth(s))
//     selectDatabase()
//   }

//   private def selectDatabase(): Unit =
//     if (database != 0)
//       select(database)
// 
//   private def authenticate(): Unit =
//     secret.foreach(auth _)

}

class RedisClient(
    override val host: String,
    override val port: Int,
    override val database: Int = 0,
    override val secret: Option[Any] = None,
    override val timeout: Int = 0,
    override val sslContext: Option[SSLContext] = None
) extends Redis with AutoCloseable {
    // with PubSub {
  override def onConnect: Unit = ??? 

  def this() = this("localhost", 6379)
  def this(connectionUri: java.net.URI) = this(
    host = connectionUri.getHost,
    port = connectionUri.getPort,
    database = RedisClient.extractDatabaseNumber(connectionUri),
    secret = Option(connectionUri.getUserInfo)
      .flatMap(_.split(':') match {
        case Array(_, password, _*) => Some(password)
        case _                      => None
      })
  )
  override def toString: String = host + ":" + String.valueOf(port) + "/" + database
  override def close(): Unit = { disconnect; () }
}