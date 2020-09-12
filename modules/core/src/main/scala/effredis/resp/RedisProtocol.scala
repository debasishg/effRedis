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

package effredis.resp

import effredis.codecs.Parse
import Parse.{ Implicits => Parsers }

// base redis value
sealed trait RedisValue

// base redis value holding a single value
sealed trait SingleRedisValue extends RedisValue {
  def value: Array[Byte]
}
// simple string
case class RedisSimpleString(value: Array[Byte]) extends SingleRedisValue
// bulk string
case class RedisBulkString(value: Array[Byte]) extends SingleRedisValue
// redis array that is homogeneous and single level (no nested arrays)
case class RedisFlatArray(value: List[SingleRedisValue]) extends RedisValue {
  def map[T](implicit parse: Parse[T]): List[Option[T]] =
    value.map { e =>
      try {
        if (e.value == RespValues.NULL_BULK_STRING) None
        else Some(parse(e.value))
      } catch {
        case _: Exception => None
      }
    }
}

// import scala.util.Try

// integer value
case class RedisInteger(value: Long) extends RedisValue
// general redis array
case class RedisArray(value: List[RedisValue]) extends RedisValue {
  def map[T](implicit parse: Parse[T]): List[Any] =
    value.map {
      case RedisInteger(v) => Some(v)
      case RedisSimpleString(value) =>
        try { Some(parse(value)) }
        catch { case _: Exception => None }
      case RedisBulkString(value) =>
        try {
          if (value == RespValues.NULL_BULK_STRING) None
          else Some(parse(value))
        } catch { case e: Exception => e.printStackTrace; None }
      case a @ RedisArray(_)     => a.map
      case a @ RedisFlatArray(_) => a.map
    }
}

// Response codes from the Redis server
object RespValues {
  final val ERR              = '-'
  final val OK               = "OK".getBytes("UTF-8")
  final val QUEUED           = "QUEUED".getBytes("UTF-8")
  final val SIMPLE_STRING_ID = '+'
  final val BULK_STRING_ID   = '$'
  final val ARRAY_ID         = '*'
  final val INTEGER_ID       = ':'
  final val LS               = "\r\n".getBytes("UTF-8")
  final val REDIS_NIL        = "$-1\r\n"
  final val NULL_BULK_STRING = REDIS_NIL.getBytes("UTF-8")
  case object RedisNil
  final val RedisNilBytes = "__RedisNil__".getBytes("UTF-8")
}

import RespValues._
object Request {
  def request(args: Seq[Array[Byte]]): Array[Byte] = {
    val b          = new scala.collection.mutable.ArrayBuffer[Byte]
    val nonNilArgs = args.filter(_ != RedisNilBytes)
    b ++= "*%d".format(nonNilArgs.size).getBytes
    b ++= LS
    nonNilArgs foreach { arg =>
      b ++= s"${BULK_STRING_ID}%d".format(arg.size).getBytes
      b ++= LS
      b ++= arg
      b ++= LS
    }
    b.toArray
  }
}

case class RedisConnectionException(message: String) extends RuntimeException(message)

/**
  * In RESP, the type of some data depends on the first byte:
  *
  * - For Simple Strings the first byte of the reply is "+"
  * - For Errors the first byte of the reply is "-"
  * - For Integers the first byte of the reply is ":"
  * - For Bulk Strings the first byte of the reply is "$"
  * - For Arrays the first byte of the reply is "*"
  *
  */
trait Reply {
  type Reply[+T]      = PartialFunction[(Char, Array[Byte]), T]
  type IntegerReply   = Reply[RedisInteger]
  type SingleReply    = Reply[SingleRedisValue]
  type ArrayReply     = Reply[RedisArray]
  type FlatArrayReply = Reply[RedisFlatArray]
  type PairReply      = Reply[Option[(SingleRedisValue, RedisFlatArray)]]

  def readLine: Array[Byte]
  def readCounted(c: Int): Array[Byte]

  val integerReply: IntegerReply = {
    case (INTEGER_ID, str) => RedisInteger(Parsers.parseLong(str))
  }

  val simpleStringReply: SingleReply = {
    case (SIMPLE_STRING_ID, str) => RedisSimpleString(str)
    case (INTEGER_ID, str)       => RedisSimpleString(str)
  }

  val bulkStringReply: SingleReply = {
    case (BULK_STRING_ID, str) => RedisBulkString(bulkRead(str))
  }

  private def bulkRead(s: Array[Byte]): Array[Byte] = {
    val size = Parsers.parseInt(s)
    size match {
      // this means we have received "$-1\r\n" which is the null bulk string
      case -1 => NULL_BULK_STRING
      case l =>
        val str = readCounted(l)
        val _   = readLine // trailing newline
        str
    }
  }

  // we treat such simple arrays differently to make it more type-safe
  // so it can be transformed into a List[T]. Generic redis array will always
  // give me a List[Any]
  val flatArrayReply: FlatArrayReply = {
    case (ARRAY_ID, str) => {
      val size = Parsers.parseInt(str)
      size match {
        // this means we have received "$-1\r\n" which is the null bulk string
        case -1 => RedisFlatArray(List(RedisBulkString(NULL_BULK_STRING)))
        case n =>
          RedisFlatArray(
            List.fill(n)(
              receive(
                simpleStringReply orElse bulkStringReply
              )
            )
          )
      }
    }
  }

  val arrayReply: ArrayReply = {
    case (ARRAY_ID, str) => {
      val size = Parsers.parseInt(str)
      size match {
        case -1 => RedisArray(List(RedisBulkString(NULL_BULK_STRING)))
        case n =>
          RedisArray(
            List.fill(n)(
              receive(
                integerReply orElse
                    simpleStringReply orElse
                    bulkStringReply orElse
                    arrayReply
              )
            )
          )
      }
    }
  }

  val pairBulkReply: PairReply = {
    case (ARRAY_ID, str) =>
      Parsers.parseInt(str) match {
        case 2 => Some((receive(bulkStringReply orElse simpleStringReply), receive(flatArrayReply)))
        case _ => None
      }
  }

  def execReply(handlers: Seq[() => Any]): PartialFunction[(Char, Array[Byte]), Option[List[Any]]] = {
    case (ARRAY_ID, str) =>
      Parsers.parseInt(str) match {
        case -1 => None
        case n if n == handlers.size =>
          Some(
            handlers.map { h =>
              try {
                h.apply()
              } catch {
                case e: Exception => e.getMessage()
              }
            }.toList
          )
        case n => throw new Exception("Protocol error: Expected " + handlers.size + " results, but got " + n)
      }
  }

  val errReply: Reply[Nothing] = {
    case (ERR, s) => throw new Exception(Parsers.parseString(s))
    case x        => throw new Exception("Protocol error: Got " + x + " as initial reply byte")
  }

  def receive[T](pf: Reply[T]): T = readLine match {
    case null =>
      throw new RedisConnectionException("Connection dropped ..")
    case line => {
      (pf orElse errReply) apply ((line(0).toChar, line.slice(1, line.length)))
    }
  }
}

trait R extends Reply {
  def asSimpleString: String = Parsers.parseString(receive(simpleStringReply).value)
  def asBoolean: Boolean =
    if (asSimpleString == "OK") true else false

  def asBulkString[T](implicit parse: Parse[T]): Option[T] =
    receive(bulkStringReply).value match {
      case NULL_BULK_STRING => None
      case v                => Some(parse(v))
    }

  def asBulkWithTime[T](implicit parse: Parse[T]): Option[T] = receive(bulkStringReply orElse flatArrayReply) match {
    case s: SingleRedisValue => Some(parse(s.value))
    case _                   => None
  }

  def asInteger: Long = receive(integerReply).value

  def asList[T](implicit parse: Parse[T]): List[Any] = receive(arrayReply).map

  def asFlatList[T](implicit parse: Parse[T]): List[Option[T]] = receive(flatArrayReply).map

  def asSet[T: Parse]: Set[Option[T]] = asFlatList.toSet

  def asFlatListPairs[A, B](implicit parseA: Parse[A], parseB: Parse[B]): List[(A, B)] =
    receive(flatArrayReply).value
      .grouped(2)
      .flatMap {
        case List(a, b) => Iterator.single((parseA(a.value), parseB(b.value)))
      }
      .toList

  def asPair[T](implicit parse: Parse[T]): Option[(Int, List[Option[T]])] =
    receive(pairBulkReply) match {
      case Some((single, multi)) => Some((Parsers.parseInt(single.value), multi.map))
      case _                     => None
    }

  def asExec(handlers: Seq[() => Any]): Option[List[Any]] = receive(execReply(handlers))
}

trait Protocol extends R

object Main extends App with R {
  val resp = "$6\r\nfoobar\r\n"
  val arr  = resp.split("\r\n")

  // val respArray = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
  val respArray =
    "*4\r\n:5461\r\n:10922\r\n*3\r\n$9\r\n127.0.0.1\r\n:7001\r\n$40\r\n677eadd2d4e1370cf42994a263a9f5b038d2bc23\r\n*3\r\n$9\r\n127.0.0.1\r\n:7005\r\n$40\r\n30849963e7a43960cea0e443b054ea8d55046192\r\n"
  // val respArray = "*3\r\n:5461\r\n:10922\r\n*3\r\n$9\r\n127.0.0.1\r\n:7001\r\n$40\r\n677eadd2d4e1370cf42994a263a9f5b038d2bc23\r\n"
  val arrArray = respArray.split("\r\n")

  var cursor = 0

  def readLine: Array[Byte] = {
    val r = arrArray(cursor).getBytes("UTF-8")
    cursor += 1
    r
  }

  def readCounted(n: Int): Array[Byte] = {
    val r = arrArray(cursor).substring(0, n).getBytes("UTF-8")
    cursor += 1
    r
  }
  println(asList)
}
