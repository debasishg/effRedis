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

package effredis.cluster.util

import io.chrisdavenport.cormorant._
import io.chrisdavenport.cormorant.Error.ParseFailure
import io.chrisdavenport.cormorant.parser._

object parsers {
  object SSVParser extends CSVLikeParser(' ')
  def parseSSVField(text: String): Either[ParseFailure, CSV.Field] = parseField(text, SSVParser)

  def parseSSVRow(text: String): Either[ParseFailure, CSV.Row] = parseRow(text, SSVParser)

  def parseSSVHeader(text: String): Either[ParseFailure, CSV.Header] = parseHeader(text, SSVParser)

  def parseSSVHeaders(text: String): Either[ParseFailure, CSV.Headers] =
    parseHeaders(text, SSVParser)

  def parseSSVRows(text: String, cleanup: Boolean = true): Either[ParseFailure, CSV.Rows] =
    parseRows(text, cleanup, SSVParser)

  def parseSSVComplete(text: String, cleanup: Boolean = true): Either[ParseFailure, CSV.Complete] =
    parseComplete(text, cleanup, SSVParser)
}
