/* =========================================================================================
 * Copyright © 2013-2014 the kamon project <http://kamon.io/>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
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

import java.util.concurrent.TimeUnit.{ NANOSECONDS ⇒ nanos }
import kamon.Kamon
import kamon.elasticsearch.{ ElasticsearchExtension, Elasticsearch }
import kamon.elasticsearch.metric.RequestsMetrics
import kamon.trace.{ Tracer, TraceContext, SegmentCategory }
import org.aspectj.lang.ProceedingJoinPoint
import org.aspectj.lang.annotation.{ Around, Aspect, Pointcut }
import org.slf4j.LoggerFactory
import scala.util.control.NonFatal
import org.elasticsearch.action._
import org.elasticsearch.action.index._
import org.elasticsearch.action.search._
import org.elasticsearch.action.get._
import org.elasticsearch.action.update._
import scala.language.existentials
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryAction
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.client.ElasticsearchClient
import org.elasticsearch.action.support.DelegatingActionListener
import kamon.trace.Segment
import org.elasticsearch.action.support.PlainActionFuture
import org.elasticsearch.action.support.AdapterActionFuture
import org.elasticsearch.common.util.concurrent.BaseFuture

@Aspect
class RequestInstrumentation {

  import RequestInstrumentation._

  @Pointcut("execution(* org.elasticsearch.client.ElasticsearchClient.execute*(..)) && args(action, request, listener)")
  def onExecuteListener[Client <: ElasticsearchClient[Client], Request <: ActionRequest[Request], Response <: ActionResponse, RequestBuilder <: ActionRequestBuilder[Request, Response, RequestBuilder, Client]](action: Action[Request, Response, RequestBuilder, Client], request: Request, listener: ActionListener[Response]): Unit = {}

  @Around("onExecuteListener(action, request, listener) ")
  def aroundExecuteListener[Client <: ElasticsearchClient[Client], Request <: ActionRequest[Request], Response <: ActionResponse, RequestBuilder <: ActionRequestBuilder[Request, Response, RequestBuilder, Client]](pjp: ProceedingJoinPoint, action: Action[Request, Response, RequestBuilder, Client], request: Request, listener: ActionListener[Response]): Unit = {
    Tracer.currentContext.collect { ctx ⇒
      val elasticsearchExtension = Kamon(Elasticsearch)
      implicit val requestRecorder = Kamon.metrics.entity(RequestsMetrics, "elasticsearch-requests")
      val segment = generateSegment(ctx, request, elasticsearchExtension)
      val start = System.nanoTime()

      pjp.proceed(Array(action, request, new ActionListener[Response] {
        def onFailure(e: Throwable): Unit = { requestRecorder.errors.increment(); segment.finish(); listener.onFailure(e) }
        def onResponse(response: Response): Unit = { recordTrace(request, response, start, elasticsearchExtension); segment.finish(); listener.onResponse(response) }
      }))

    }
  } getOrElse pjp.proceed()

  @Pointcut("execution(* org.elasticsearch.client.ElasticsearchClient.execute*(..)) && args(action, request)")
  def onExecute[Client <: ElasticsearchClient[Client], Request <: ActionRequest[Request], Response <: ActionResponse, RequestBuilder <: ActionRequestBuilder[Request, Response, RequestBuilder, Client]](action: Action[Request, Response, RequestBuilder, Client], request: Request): Unit = {}

  @Around("onExecute(action, request)")
  def aroundExecute[Client <: ElasticsearchClient[Client], Request <: ActionRequest[Request], Response <: ActionResponse, RequestBuilder <: ActionRequestBuilder[Request, Response, RequestBuilder, Client]](pjp: ProceedingJoinPoint, action: Action[Request, Response, RequestBuilder, Client], request: Request) = {
    Tracer.currentContext.collect { ctx ⇒
      val elasticsearchExtension = Kamon(Elasticsearch)
      implicit val requestRecorder = Kamon.metrics.entity(RequestsMetrics, "elasticsearch-requests")
      val segment = generateSegment(ctx, request, elasticsearchExtension)
      val start = System.nanoTime()

      val r: ActionFuture[Response] = pjp.proceed().asInstanceOf[ActionFuture[Response]];

      new PlainActionFuture[Response] {
        override def get(): Response = {
          try {
            val response = r.get(); recordTrace(request, response, start, elasticsearchExtension); response
          } catch { case NonFatal(e) ⇒ requestRecorder.errors.increment(); throw e; }
          finally {
            segment.finish();
          }
        }
      }
    }
  } getOrElse pjp.proceed()

  def recordTrace[Request <: ActionRequest[Request], Response <: ActionResponse](request: Request, response: Response, start: Long, elasticsearchExtension: ElasticsearchExtension)(implicit requestRecorder: RequestsMetrics) {
    val timeSpent = System.nanoTime() - start
    request match {
      case r: GetRequest    ⇒ requestRecorder.reads.record(timeSpent)
      case r: SearchRequest ⇒ requestRecorder.reads.record(timeSpent)
      case r: IndexRequest  ⇒ requestRecorder.writes.record(timeSpent)
      case r: UpdateRequest ⇒ requestRecorder.writes.record(timeSpent)
      case r: DeleteRequest ⇒ requestRecorder.writes.record(timeSpent)
      case _ ⇒
        log.debug(s"Unable to parse request [$request]")
    }

    val timeSpentInMillis = nanos.toMillis(timeSpent)

    if (timeSpentInMillis >= elasticsearchExtension.slowQueryThreshold) {
      requestRecorder.slows.increment()
      elasticsearchExtension.processSlowQuery(request, timeSpentInMillis)
    }
  }

  def generateSegment(ctx: TraceContext, request: ActionRequest[_], elasticsearchExtension: ElasticsearchExtension) = {
    val segmentName = elasticsearchExtension.generateElasticsearchSegmentName(request)
    val segment = ctx.startSegment(segmentName, SegmentCategory.Database, Elasticsearch.SegmentLibraryName)
    segment
  }
}

object RequestInstrumentation {
  val log = LoggerFactory.getLogger(classOf[RequestInstrumentation])
}
