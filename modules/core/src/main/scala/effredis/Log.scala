package effredis

import org.slf4j.LoggerFactory

trait Log {
  private val log = LoggerFactory.getLogger(getClass)

  def ifTrace(message: => String): Unit = if (log.isTraceEnabled) trace(message)
  def trace(message: String, values: AnyRef*): Unit =
    log.trace(message, values: _*)
  def trace(message: String, error: Throwable): Unit = log.trace(message, error)

  def ifDebug(message: => String): Unit = if (log.isDebugEnabled) debug(message)
  def debug(message: String, values: AnyRef*): Unit =
    log.debug(message, values: _*)
  def debug(message: String, error: Throwable): Unit = log.debug(message, error)

  def ifInfo(message: => String): Unit = if (log.isInfoEnabled) info(message)
  def info(message: String, values: AnyRef*): Unit =
    log.info(message, values: _*)
  def info(message: String, error: Throwable): Unit = log.info(message, error)

  def ifWarn(message: => String): Unit = if (log.isWarnEnabled) warn(message)
  def warn(message: String, values: AnyRef*): Unit =
    log.warn(message, values: _*)
  def warn(message: String, error: Throwable): Unit = log.warn(message, error)

  def ifError(message: => String): Unit = if (log.isErrorEnabled) error(message)
  def error(message: String, values: AnyRef*): Unit =
    log.error(message, values: _*)
  def error(message: String, error: Throwable): Unit = log.error(message, error)
}
