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

import java.io._
import java.net.{ InetSocketAddress, Socket, SocketTimeoutException }
import javax.net.ssl.SSLContext

// import codecs.Parse.parseStringSafe

trait RedisIO { // extends Log {
  val host: String
  val port: Int
  val timeout: Int

  val sslContext: Option[SSLContext] = None

  var socket: Socket    = _
  var out: OutputStream = _
  var in: InputStream   = _
  var db: Int           = _

  def connected: Boolean =
    socket != null && socket.isBound && !socket.isClosed && socket.isConnected && !socket.isInputShutdown && !socket.isOutputShutdown

  def onConnect(): Unit

  // Connects the socket, and sets the input and output streams.
  def connect: Boolean =
    try {
      socket = new Socket()
      socket.connect(new InetSocketAddress(host, port), timeout)

      socket.setSoTimeout(timeout)
      socket.setKeepAlive(true)
      socket.setTcpNoDelay(true)

      sslContext.foreach(sc => socket = sc.getSocketFactory().createSocket(socket, host, port, true))

      out = socket.getOutputStream
      in = new BufferedInputStream(socket.getInputStream)
      onConnect()
      true
    } catch {
      case x: Throwable =>
        clearFd()
        throw new RuntimeException(x)
    }

  // Disconnects the socket.
  def disconnect: Boolean =
    try {
      socket.close()
      out.close()
      in.close()
      clearFd()
      true
    } catch {
      case _: Throwable =>
        false
    }

  def clearFd(): Unit = {
    socket = null
    out = null
    in = null
  }

  // Wrapper for the socket write operation.
  def write_to_socket(op: OutputStream => Unit): Unit = op(out)

  // Writes data to a socket using the specified block.
  def write(data: Array[Byte]): Unit = {
    // ifDebug("C: " + parseStringSafe(data))
    if (!connected) connect
    write_to_socket { os =>
      try {
        os.write(data)
        os.flush()
      } catch {
        case _: Throwable => throw new RedisConnectionException("connection is closed. write error")
      }
    }
  }

  private val crlf = List(13, 10)

  def readLine: Array[Byte] = {
    if (!connected) connect
    var delimiter        = crlf
    var found: List[Int] = Nil
    val build            = new scala.collection.mutable.ArrayBuilder.ofByte
    try {
      while (delimiter != Nil && in != null) {
        val next = in.read
        if (next < 0) return null
        if (next == delimiter.head) {
          found ::= delimiter.head
          delimiter = delimiter.tail
        } else {
          if (found != Nil) {
            delimiter = crlf
            build ++= found.reverseIterator.map(_.toByte).toList
            found = Nil
          }
          build += next.toByte
        }
      }
    } catch {
      case ex: SocketTimeoutException if timeout > 0 =>
        // can't ignore response without a blocking socket-read so we close this socket instead
        disconnect
        throw ex
    }
    build.result
  }

  def readCounted(count: Int): Array[Byte] = {
    if (!connected) connect
    val arr = new Array[Byte](count)
    var cur = 0
    try {
      while (cur < count) {
        cur += in.read(arr, cur, count - cur)
      }
    } catch {
      case ex: SocketTimeoutException if timeout > 0 =>
        // can't ignore response without a blocking socket-read so we close this socket instead
        disconnect
        throw ex
    }
    arr
  }
}
