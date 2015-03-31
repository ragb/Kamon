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

@Aspect
class RequestInstrumentation {

  import RequestInstrumentation._

  @Pointcut("call(* org.elasticsearch.action.ActionRequestBuilder.execute(..))")
  def onExecuteStatement(): Unit = {}

  @Around("onExecuteStatement()")
  def aroundExecuteStatement(pjp: ProceedingJoinPoint): Any = {
    Tracer.currentContext.collect { ctx ⇒
      val elasticsearchExtension = Kamon(Elasticsearch)
      implicit val statementRecorder = Kamon.metrics.entity(RequestsMetrics, "elasticsearch-requests")

      val request = pjp.getTarget().asInstanceOf[ActionRequestBuilder[_ with ActionRequest[_], ActionResponse, _, _]].request()

      request match {
        case r: GetRequest    ⇒ withSegment(ctx, r, elasticsearchExtension)(recordRead(pjp, r, elasticsearchExtension))
        case r: IndexRequest  ⇒ withSegment(ctx, r, elasticsearchExtension)(recordWrite(pjp, r, elasticsearchExtension))
        case r: UpdateRequest ⇒ withSegment(ctx, r, elasticsearchExtension)(recordWrite(pjp, r, elasticsearchExtension))
        case r: DeleteRequest ⇒ withSegment(ctx, r, elasticsearchExtension)(recordWrite(pjp, r, elasticsearchExtension))
        case _ ⇒
          log.debug(s"Unable to parse request [$request]")
          pjp.proceed()
      }
    }
  } getOrElse pjp.proceed()

  def withTimeSpent[A](thunk: ⇒ A)(timeSpent: Long ⇒ Unit): A = {
    val start = System.nanoTime()
    try thunk finally timeSpent(System.nanoTime() - start)
  }

  def withSegment[A](ctx: TraceContext, request: ActionRequest[_], elasticsearchExtension: ElasticsearchExtension)(thunk: ⇒ A): A = {
    val segmentName = elasticsearchExtension.generateElasticsearchSegmentName(request)
    val segment = ctx.startSegment(segmentName, SegmentCategory.Database, Elasticsearch.SegmentLibraryName)
    try thunk finally segment.finish()
  }

  def recordRead[T <: ActionResponse](pjp: ProceedingJoinPoint, request: ActionRequest[_], elasticsearchExtension: ElasticsearchExtension)(implicit statementRecorder: RequestsMetrics): Any = {
    val result = pjp.proceed().asInstanceOf[ListenableActionFuture[T]]
    withTimeSpent(result) { timeSpent ⇒
      result.addListener(new ActionListener[T] {
        def onFailure(e: Throwable): Unit = { statementRecorder.errors.increment() }
        def onResponse(response: T): Unit = { statementRecorder.reads.record(timeSpent) }
      })

      val timeSpentInMillis = nanos.toMillis(timeSpent)

      if (timeSpentInMillis >= elasticsearchExtension.slowQueryThreshold) {
        statementRecorder.slows.increment()
        elasticsearchExtension.processSlowQuery(request, timeSpentInMillis)
      }
    }
  }

  def recordWrite[T <: ActionResponse](pjp: ProceedingJoinPoint, request: ActionRequest[_], elasticsearchExtension: ElasticsearchExtension)(implicit statementRecorder: RequestsMetrics): Any = {
    val result = pjp.proceed().asInstanceOf[ListenableActionFuture[T]]
    withTimeSpent(result) { timeSpent ⇒
      result.addListener(new ActionListener[T] {
        def onFailure(e: Throwable): Unit = { statementRecorder.errors.increment() }
        def onResponse(response: T): Unit = { statementRecorder.writes.record(timeSpent) }
      })
    }
  }
}

object RequestInstrumentation {
  val log = LoggerFactory.getLogger(classOf[RequestInstrumentation])
}
