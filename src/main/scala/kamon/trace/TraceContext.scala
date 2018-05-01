package kamon.trace

import kamon.Kamon
import kamon.context.{ Codecs, Context, Key, TextMap }
import kamon.trace.SpanContext.SamplingDecision

class TraceContext extends Codecs.ForEntry[TextMap] {
  import TraceContext._

  override def encode(context: Context): TextMap = {
    val parentSpan = context.get(Span.ContextKey)
    val traceState = context.get(TraceState)

    if (parentSpan.nonEmpty()) {
      val carrier = TextMap.Default()
      val traceParentHeader = packTraceParent(parentSpan)
      carrier.put(TraceParentHeader, traceParentHeader)

      if (traceState.nonEmpty)
        carrier.put(TraceStateHeader, packTraceState(parentSpan, traceState.get))
      else
        carrier.put(TraceStateHeader, SystemName + "=" + traceParentHeader)

      carrier

    } else TextMap.Default()
  }

  override def decode(carrier: TextMap, context: Context): Context = {
    val traceContext = for {
      traceParent <- carrier.get(TraceParentHeader)
      traceState <- carrier.get(TraceStateHeader)
      traceContextData <- unpackTraceContext(traceParent, traceState)
    } yield {
      context
        .withKey(Span.ContextKey, traceContextData.parent)
        .withKey(TraceContext.TraceState, Some(traceContextData))
    }

    traceContext.getOrElse(context)
  }

  private def unpackTraceContext(traceParent: String, traceState: String): Option[TraceContextData] = {
    // very naive and permissive unpacking
    for {
      span <- unpackTraceParent(traceParent)
      traceState <- unpackTraceState(traceState)
    } yield {
      TraceContextData(span, traceState)
    }
  }

  private def unpackTraceParent(traceParent: String): Option[Span] = {
    val traceParentComponents = traceParent.split("\\-")
    if (traceParentComponents.length != 4) None else {
      val remoteSpanContext = SpanContext(
        traceID = unpackTraceID(traceParentComponents(1)),
        spanID = unpackSpanID(traceParentComponents(2)),
        parentID = IdentityProvider.NoIdentifier,
        samplingDecision = unpackSamplingDecision(traceParentComponents(3))
      )

      if (remoteSpanContext.traceID == IdentityProvider.NoIdentifier || remoteSpanContext.spanID == IdentityProvider.NoIdentifier)
        None
      else
        Some(Span.Remote(remoteSpanContext))
    }
  }

  private def packTraceParent(parent: Span): String = {
    val spanContext = parent.context()
    val samplingDecision = if (spanContext.samplingDecision == SamplingDecision.Sample) "01" else "00"

    s"00-${spanContext.traceID.string}-${spanContext.spanID.string}-${samplingDecision}"
  }

  private def packTraceState(actualParent: Span, traceContextData: TraceContextData): String = {
    def packPairs(pairs: Seq[(String, String)]): String = pairs.map { case (key, value) => key + "=" + value } mkString (",")

    if (actualParent == traceContextData.parent) {
      packPairs(traceContextData.state)
    } else {
      val stateWithoutSelf = traceContextData.state.filter { case (key, value) => key != SystemName }
      packPairs((SystemName -> packTraceParent(actualParent)) +: stateWithoutSelf)
    }
  }

  private def unpackTraceState(traceState: String): Option[Seq[(String, String)]] = {
    if (traceState.length == 0) None else {
      val stateComponents = traceState.split("\\,")
      val unpackedTraceState = stateComponents.map(state => {
        val separatorIndex = state.indexOf('=')
        (state.substring(0, separatorIndex), state.substring(separatorIndex + 1))
      })

      Some(unpackedTraceState)
    }
  }

  private def unpackTraceID(id: String): IdentityProvider.Identifier =
    Kamon.identityProvider.traceIdGenerator().from(id)

  private def unpackSpanID(id: String): IdentityProvider.Identifier =
    Kamon.identityProvider.spanIdGenerator().from(id)

  private def unpackSamplingDecision(decision: String): SamplingDecision =
    if ("01" == decision) SamplingDecision.Sample else SamplingDecision.Unknown
}

object TraceContext {
  val SystemName = "kamon"
  val TraceParentHeader = "traceparent"
  val TraceStateHeader = "tracestate"
  val TraceState = Key.local[Option[TraceContextData]]("tracestate", None)

  case class TraceContextData(parent: Span, state: Seq[(String, String)])

}
