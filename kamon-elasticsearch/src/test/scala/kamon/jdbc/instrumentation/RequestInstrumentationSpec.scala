/* =========================================================================================
 * Copyright © 2013-2014 the kamon project <http://kamon.io/>
 *
 * Licensed under the Apache License, Version 2.0 (the "License") you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * =========================================================================================
 */

package kamon.elasticsearch.instrumentation

import com.typesafe.config.ConfigFactory
import kamon.elasticsearch.{ Elasticsearch, ElasticsearchNameGenerator, ElasticErrorProcessor, SlowQueryProcessor }
import kamon.metric.TraceMetricsSpec
import kamon.testkit.BaseKamonSpec
import kamon.trace.{ Tracer, SegmentCategory }
import org.elasticsearch.node.NodeBuilder._
import org.elasticsearch.action._
import org.elasticsearch.action.index.IndexRequestBuilder
import org.elasticsearch.indices.InvalidIndexNameException

class RequestInstrumentationSpec extends BaseKamonSpec("elasticsearch-spec") {
  override lazy val config =
    ConfigFactory.parseString(
      """
        |kamon {
        |   elasticsearch {
        |     slow-query-threshold = 100 milliseconds
        |
        |     # Fully qualified name of the implementation of kamon.elasticsearch.SlowQueryProcessor.
        |     slow-query-processor = kamon.elasticsearch.instrumentation.NoOpSlowQueryProcessor
        |
        |     # Fully qualified name of the implementation of kamon.elasticsearch.SqlErrorProcessor.
        |     elasticsearch-error-processor = kamon.elasticsearch.instrumentation.NoOpElasticErrorProcessor
        |
        |     # Fully qualified name of the implementation of kamon.elasticsearch.JdbcNameGenerator
        |     name-generator = kamon.elasticsearch.instrumentation.NoOpElasticsearchNameGenerator
        |   }
        |}
      """.stripMargin)

  val node = nodeBuilder().local(true).node();
  val client = node.client();

  "the RequestInstrumentation" should {
    "record the execution time of INDEX operation" in {
      Tracer.withContext(newContext("elasticsearch-trace-index")) {
        for (id ← 1 to 100) {
          client.prepareIndex("twitter", "tweet", id.toString)
            .setSource("{" +
              "\"user\":\"kimchy\"," +
              "\"postDate\":\"2013-01-30\"," +
              "\"message\":\"trying out Elasticsearch\"" +
              "}")
            .execute().actionGet()
        }

        Tracer.currentContext.finish()
      }

      val elasticsearchSnapshot = takeSnapshotOf("elasticsearch-requests", "elasticsearch-requests")
      elasticsearchSnapshot.histogram("writes").get.numberOfMeasurements should be(100)

      val traceSnapshot = takeSnapshotOf("elasticsearch-trace-index", "trace")
      traceSnapshot.histogram("elapsed-time").get.numberOfMeasurements should be(1)

      val segmentSnapshot = takeSnapshotOf("Elasticsearch[IndexRequest]", "trace-segment",
        tags = Map(
          "trace" -> "elasticsearch-trace-index",
          "category" -> SegmentCategory.Database,
          "library" -> Elasticsearch.SegmentLibraryName))

      segmentSnapshot.histogram("elapsed-time").get.numberOfMeasurements should be(100)
    }

    "record the execution time of GET operation" in {
      Tracer.withContext(newContext("elasticsearch-trace-get")) {
        for (id ← 1 to 100) {
          client.prepareGet("twitter", "tweet", id.toString).execute().actionGet()
        }

        Tracer.currentContext.finish()
      }

      val elasticsearchSnapshot = takeSnapshotOf("elasticsearch-requests", "elasticsearch-requests")
      elasticsearchSnapshot.histogram("reads").get.numberOfMeasurements should be(100)

      val traceSnapshot = takeSnapshotOf("elasticsearch-trace-get", "trace")
      traceSnapshot.histogram("elapsed-time").get.numberOfMeasurements should be(1)

      val segmentSnapshot = takeSnapshotOf("Elasticsearch[GetRequest]", "trace-segment",
        tags = Map(
          "trace" -> "elasticsearch-trace-get",
          "category" -> SegmentCategory.Database,
          "library" -> Elasticsearch.SegmentLibraryName))

      segmentSnapshot.histogram("elapsed-time").get.numberOfMeasurements should be(100)
    }

    "record the execution time of UPDATE operation" in {
      Tracer.withContext(newContext("elasticsearch-trace-update")) {
        for (id ← 1 to 100) {
          client.prepareUpdate("twitter", "tweet", id.toString)
            .setDoc("{" +
              "\"updated\":\"updated\"" +
              "}")
            .execute().actionGet()
        }

        Tracer.currentContext.finish()
      }

      val elasticsearchSnapshot = takeSnapshotOf("elasticsearch-requests", "elasticsearch-requests")
      elasticsearchSnapshot.histogram("writes").get.numberOfMeasurements should be(100)

      val traceSnapshot = takeSnapshotOf("elasticsearch-trace-update", "trace")
      traceSnapshot.histogram("elapsed-time").get.numberOfMeasurements should be(1)

      val segmentSnapshot = takeSnapshotOf("Elasticsearch[UpdateRequest]", "trace-segment",
        tags = Map(
          "trace" -> "elasticsearch-trace-update",
          "category" -> SegmentCategory.Database,
          "library" -> Elasticsearch.SegmentLibraryName))

      segmentSnapshot.histogram("elapsed-time").get.numberOfMeasurements should be(100)
    }

    "record the execution time of DELETE operation" in {
      Tracer.withContext(newContext("elasticsearch-trace-delete")) {
        for (id ← 1 to 100) {
          client.prepareDelete("twitter", "tweet", id.toString).execute().actionGet()
        }

        Tracer.currentContext.finish()
      }

      val jdbcSnapshot = takeSnapshotOf("elasticsearch-requests", "elasticsearch-requests")
      jdbcSnapshot.histogram("writes").get.numberOfMeasurements should be(100)

      val traceSnapshot = takeSnapshotOf("elasticsearch-trace-delete", "trace")
      traceSnapshot.histogram("elapsed-time").get.numberOfMeasurements should be(1)

      val segmentSnapshot = takeSnapshotOf("Elasticsearch[DeleteRequest]", "trace-segment",
        tags = Map(
          "trace" -> "elasticsearch-trace-delete",
          "category" -> SegmentCategory.Database,
          "library" -> Elasticsearch.SegmentLibraryName))

      segmentSnapshot.histogram("elapsed-time").get.numberOfMeasurements should be(100)

    }
    //
    //    "record the execution time of SLOW QUERIES based on the kamon.jdbc.slow-query-threshold" in {
    //      Tracer.withContext(newContext("jdbc-trace-slow")) {
    //        for (id ← 1 to 2) {
    //          val select = s"SELECT * FROM Address; CALL SLEEP(100)"
    //          val selectStatement = connection.createStatement()
    //          selectStatement.execute(select)
    //        }
    //
    //        Tracer.currentContext.finish()
    //      }
    //
    //      val jdbcSnapshot = takeSnapshotOf("jdbc-statements", "jdbc-statements")
    //      jdbcSnapshot.counter("slows").get.count should be(2)
    //
    //    }
    //
    "count all ERRORS" in {
      Tracer.withContext(newContext("elasticsearch-trace-errors")) {
        for (id ← 1 to 10) {
          intercept[InvalidIndexNameException] {
            client.prepareDelete("index name with spaces", "tweet", id.toString).execute().actionGet()
          }
        }

        Tracer.currentContext.finish()
      }

      val jdbcSnapshot = takeSnapshotOf("elasticsearch-requests", "elasticsearch-requests")
      jdbcSnapshot.counter("errors").get.count should be(10)
    }
  }
}

class NoOpSlowQueryProcessor extends SlowQueryProcessor {
  override def process(request: ActionRequest[_], executionTimeInMillis: Long, queryThresholdInMillis: Long): Unit = { /*do nothing!!!*/ }
}

class NoOpElasticErrorProcessor extends ElasticErrorProcessor {
  override def process(request: ActionRequest[_], ex: Throwable): Unit = { /*do nothing!!!*/ }
}

class NoOpElasticsearchNameGenerator extends ElasticsearchNameGenerator {
  override def generateElasticsearchSegmentName(request: ActionRequest[_]): String = s"Elasticsearch[${request.getClass.getSimpleName}]"
}