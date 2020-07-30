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

import serialization.Parse
import Parse.{ Implicits => Parsers }

case class GeoRadiusMember(
    member: Option[String],
    hash: Option[Long] = None,
    dist: Option[String] = None,
    coords: Option[(String, String)] = None
)

private[effredis] object Commands {

  // Response codes from the Redis server
  val ERR    = '-'
  val OK     = "OK".getBytes("UTF-8")
  val QUEUED = "QUEUED".getBytes("UTF-8")
  val SINGLE = '+'
  val BULK   = '$'
  val MULTI  = '*'
  val INT    = ':'

  val LS = "\r\n".getBytes("UTF-8")

  def multiBulk(args: Seq[Array[Byte]]): Array[Byte] = {
    val b = new scala.collection.mutable.ArrayBuilder.ofByte
    b ++= "*%d".format(args.size).getBytes
    b ++= LS
    args foreach { arg =>
      b ++= "$%d".format(arg.size).getBytes
      b ++= LS
      b ++= arg
      b ++= LS
    }
    b.result
  }
}

import Commands._

case class RedisConnectionException(message: String) extends RuntimeException(message)
case class RedisMultiExecException(message: String) extends RuntimeException(message)

private[effredis] trait Reply {

  type Reply[T]         = PartialFunction[(Char, Array[Byte]), T]
  type SingleReply      = Reply[Option[Array[Byte]]]
  type MultiReply       = Reply[Option[List[Option[Array[Byte]]]]]
  type MultiNestedReply = Reply[Option[List[Option[List[Option[Array[Byte]]]]]]]
  type PairReply        = Reply[Option[(Option[Array[Byte]], Option[List[Option[Array[Byte]]]])]]

  def readLine: Array[Byte]
  def readCounted(c: Int): Array[Byte]

  val integerReply: Reply[Option[Int]] = {
    case (INT, s)                               => Some(Parsers.parseInt(s))
    case (BULK, s) if Parsers.parseInt(s) == -1 => None
  }

  val longReply: Reply[Option[Long]] = {
    case (INT, s)                               => Some(Parsers.parseLong(s))
    case (BULK, s) if Parsers.parseInt(s) == -1 => None
  }

  val singleLineReply: SingleReply = {
    case (SINGLE, s) => Some(s)
    case (INT, s)    => Some(s)
  }

  def bulkRead(s: Array[Byte]): Option[Array[Byte]] =
    Parsers.parseInt(s) match {
      case -1 => None
      case l =>
        val str = readCounted(l)
        val _   = readLine // trailing newline
        Some(str)
    }

  val bulkReply: SingleReply = {
    case (BULK, s) =>
      bulkRead(s)
  }

  val multiBulkReply: MultiReply = {
    case (MULTI, str) =>
      Parsers.parseInt(str) match {
        case -1 => None
        case n  => Some(List.fill(n)(receive(bulkReply orElse singleLineReply)))
      }
  }

  val multiBulkNested: MultiNestedReply = {
    case (MULTI, str) =>
      Parsers.parseInt(str) match {
        case -1 => None
        case n  => Some(List.fill(n)(receive(multiBulkReply)))
      }
  }

  val pairBulkReply: PairReply = {
    case (MULTI, str) =>
      Parsers.parseInt(str) match {
        case 2 => Some((receive(bulkReply orElse singleLineReply), receive(multiBulkReply)))
        case _ => None
      }
  }

  def execReply(handlers: Seq[() => Any]): PartialFunction[(Char, Array[Byte]), Option[List[Any]]] = {
    case (MULTI, str) =>
      Parsers.parseInt(str) match {
        case -1 => None
        case n if n == handlers.size =>
          Some(handlers.map(_.apply).toList)
        case n => throw new Exception("Protocol error: Expected " + handlers.size + " results, but got " + n)
      }
  }

  val errReply: Reply[Nothing] = {
    case (ERR, s) => throw new Exception(Parsers.parseString(s))
    case x        => throw new Exception("Protocol error: Got " + x + " as initial reply byte")
  }

  def queuedReplyInt: Reply[Option[Int]] = {
    case (SINGLE, QUEUED) => Some(Int.MaxValue)
  }

  def queuedReplyLong: Reply[Option[Long]] = {
    case (SINGLE, QUEUED) => Some(Long.MaxValue)
  }

  def queuedReplyList: MultiReply = {
    case (SINGLE, QUEUED) => Some(List(Some(QUEUED)))
  }

  def receive[T](pf: Reply[T]): T = readLine match {
    case null =>
      throw new RedisConnectionException("Connection dropped ..")
    case line =>
      (pf orElse errReply) apply ((line(0).toChar, line.slice(1, line.length)))
  }

  /**
    * The following partial functions intend to manage the response from the GEORADIUS and GEORADIUSBYMEMBER commands.
    * The code is not as generic as the previous ones as the exposed types are quite complex and really specific
    * to these two commands
    */
  type FoldReply = PartialFunction[(Char, Array[Byte], Option[GeoRadiusMember]), Option[GeoRadiusMember]]

  /**
    * dedicated errorReply working with foldReceive.
    * @tparam A
    * @return
    */
  private def errFoldReply[A]: FoldReply = {
    case (ERR, s, _) => throw new Exception(Parsers.parseString(s))
    case x           => throw new Exception("Protocol error: Got " + x + " as initial reply byte")
  }

  /**
    * dedicated receive used in our fold strategy
    * @param pf
    * @param a
    * @tparam A
    * @return
    */
  private def foldReceive[A](pf: FoldReply, a: Option[GeoRadiusMember]): Option[GeoRadiusMember] = readLine match {
    case null =>
      throw new RedisConnectionException("Connection dropped ..")
    case line =>
      (pf orElse errFoldReply) apply ((line(0).toChar, line.slice(1, line.length), a))
  }

  /**
    * Final step : we feed our accumulator with the data we find.
    *
    *  - First BULK : It is the member name
    *  - Second BULK : It is the distance to the ref point
    *  - INT : The member hash
    *  - MULTI : The member coordinates. Should contain exactly two members.
    */
  private val complexGeoRadius
      : PartialFunction[(Char, Array[Byte], Option[GeoRadiusMember]), Option[GeoRadiusMember]] = {
    case (BULK, s, a) =>
      val retrieved = bulkRead(s)
      retrieved.map { ret =>
        a.fold(GeoRadiusMember(Some(Parsers.parseString(ret)))) { some =>
          some.member.fold(GeoRadiusMember(Some(Parsers.parseString(ret)))) { _ =>
            some.copy(dist = Some(Parsers.parseString(ret)))
          }
        }
      }
    case (INT, s, opt) => opt.map(a => a.copy(hash = Some(Parsers.parseLong(s))))
    case (MULTI, s, a) =>
      Parsers.parseInt(s) match {
        case 2 =>
          val lon: Option[String] = receive(bulkReply).map(Parsers.parseString)
          val lat: Option[String] = receive(bulkReply).map(Parsers.parseString)
          a.map(_.copy(coords = Some((lon.getOrElse(""), lat.getOrElse("")))))
        case _ => None
      }
  }

  /**
    * Second step : We must manage two distinct cases:
    *
    *  - The user performed a basic search, he will only receive strings listing the members found in the radius. A
    *    BULK is exposed in this case.
    *  - The user performed a complex search (with radius, coordinates or distance in the result). A more complex data structure
    *    is exposed, as a MULTI containing a BULK, a possible BULK, a possible INT and a possible MULTI. We use a dedicated
    *    strategy for this containing MULTI that will be able to manage all the possible cases. Rather than using a List.fill here,
    *    we use a range and fold on it in order to be able to use an accumulator. This way, the accumulator is fed with each
    *    member of the multi and changed accordingly
    */
  private val singleGeoRadius: Reply[Option[GeoRadiusMember]] = {
    case (BULK, s) =>
      bulkRead(s).map(str => GeoRadiusMember(Some(Parsers.parseString(str))))
    case (MULTI, str) =>
      Parsers.parseInt(str) match {
        case -1 => None
        case n =>
          val out: Option[GeoRadiusMember] =
            List.range(0, n).foldLeft[Option[GeoRadiusMember]](None)((in, _) => foldReceive(complexGeoRadius, in))
          out
      }
  }

  /**
    * Entry point for GEORADIUS result analysis. The analysis is done in three steps.
    *
    * First step : we are expecting a MULTI structure and will iterate trivially on it.
    */
  val geoRadiusMemberReply: Reply[Option[List[Option[GeoRadiusMember]]]] = {
    case (MULTI, str) =>
      Parsers.parseInt(str) match {
        case -1 => None
        case n  => Some(List.fill(n)(receive(singleGeoRadius)))
      }
  }
}

private[effredis] trait R extends Reply {
  def asString: Option[String] = receive(singleLineReply) map Parsers.parseString

  def asBulk[T](implicit parse: Parse[T]): Option[T] = receive(bulkReply) map parse

  def asBulkWithTime[T](implicit parse: Parse[T]): Option[T] = receive(bulkReply orElse multiBulkReply) match {
    case Some(bytes: Array[Byte]) => Some(parse(bytes))
    case _                        => None
  }

  def asInt: Option[Int]   = receive(integerReply orElse queuedReplyInt)
  def asLong: Option[Long] = receive(longReply orElse queuedReplyLong)

  def asBoolean: Boolean = receive(integerReply orElse singleLineReply) match {
    case Some(n: Int) => n > 0
    case Some(s: Array[Byte]) =>
      Parsers.parseString(s) match {
        case "OK"     => true
        case "QUEUED" => true
        case _        => false
      }
    case _ => false
  }

  def asList[T](implicit parse: Parse[T]): Option[List[Option[T]]] = receive(multiBulkReply).map(_.map(_.map(parse)))

  def asListPairs[A, B](implicit parseA: Parse[A], parseB: Parse[B]): Option[List[Option[(A, B)]]] =
    receive(multiBulkReply).map(_.grouped(2).flatMap {
      case List(Some(a), Some(b)) => Iterator.single(Some((parseA(a), parseB(b))))
      case _                      => Iterator.single(None)
    }.toList)

  def asQueuedList: Option[List[Option[String]]] = receive(queuedReplyList).map(_.map(_.map(Parsers.parseString)))

  def asExec(handlers: Seq[() => Any]): Option[List[Any]] = receive(execReply(handlers))

  def asSet[T: Parse]: Option[Set[Option[T]]] = asList map (_.toSet)

  def asPair[T](implicit parse: Parse[T]): Option[(Option[Int], Option[List[Option[T]]])] =
    receive(pairBulkReply) match {
      case Some((single, multi)) => Some(((single map Parsers.parseInt), multi.map(_.map(_.map(parse)))))
      case _                     => None
    }

  def asAny = receive(integerReply orElse singleLineReply orElse bulkReply orElse multiBulkReply)
}

trait Protocol extends R
