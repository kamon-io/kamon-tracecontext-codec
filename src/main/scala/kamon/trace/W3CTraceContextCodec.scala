package kamon.trace

import kamon.context.{ Codecs, Context, Key, TextMap }
import kamon.trace.IdentityProvider.DoubleSizeTraceID
import kamon.trace.SpanContext.SamplingDecision

class W3CTraceParentCodec extends Codecs.ForEntry[TextMap] {
  import W3CTraceContext._
  private val identityProvider = new DoubleSizeTraceID()

  override def encode(context: Context): TextMap = {
    val carrier = TextMap.Default()
    val spanInContext = context.get(Span.ContextKey)

    if (spanInContext.nonEmpty()) {
      val traceParentData = encodeTraceParent(spanInContext)
      carrier.put(TraceParentHeaderName, traceParentData)

      if(context.get(W3CTraceContext.TraceStateKey).isEmpty) {
        carrier.put(TraceStateHeaderName, OwnSystemName + "=" + traceParentData)
      }

    } else
      context.get(W3CTraceContext.TraceStateKey).foreach(traceState => {
        carrier.put(TraceParentHeaderName, encodeTraceParent(traceState.parent))
      })

    carrier
  }

  override def decode(carrier: TextMap, context: Context): Context = {
    val contextWithParent = for {
      _             <- carrier.get(TraceParentHeaderName)
      traceState    <- carrier.get(TraceStateHeaderName)
      ownTraceState <- decodeTraceState(traceState).flatMap(findOwnSystemState)
      parentSpan    <- decodeTraceParent(ownTraceState, identityProvider)
    } yield {
      context.withKey(Span.ContextKey, parentSpan)
    }

    contextWithParent getOrElse context
  }

  private def findOwnSystemState(pairs: Seq[(String, String)]): Option[String] =
    pairs.find { case (k, _) => k == OwnSystemName } map (_._2)
}

class W3CTraceStateCodec extends Codecs.ForEntry[TextMap] {
  import W3CTraceContext._
  private val identityProvider = new DoubleSizeTraceID()

  override def encode(context: Context): TextMap = {
    val carrier = TextMap.Default()
    val spanInContext = context.get(Span.ContextKey)

    if (spanInContext.nonEmpty()) {
      val ownTraceState = Seq(OwnSystemName -> encodeTraceParent(spanInContext))

      val traceStateData = context.get(W3CTraceContext.TraceStateKey) match {
        case Some(traceState) => ownTraceState ++: (traceState.state.filter { case (k, v) => k != OwnSystemName })
        case None             => ownTraceState
      }

      carrier.put(TraceStateHeaderName, packPairs(traceStateData))
    } else {
      context.get(W3CTraceContext.TraceStateKey).foreach { traceState =>
        carrier.put(TraceParentHeaderName, encodeTraceParent(traceState.parent))
        carrier.put(TraceStateHeaderName, packPairs(traceState.state))
      }
    }


    carrier
  }

  override def decode(carrier: TextMap, context: Context): Context = {
    val contextWithTraceState = for {
      traceParent     <- carrier.get(TraceParentHeaderName)
      traceState      <- carrier.get(TraceStateHeaderName)
      traceStatePairs <- decodeTraceState(traceState)
      parentSpan      <- decodeTraceParent(traceParent, identityProvider) if(traceStatePairs.length > 0)
    } yield {
      val parentOwner = traceStatePairs.head._1
      context.withKey(W3CTraceContext.TraceStateKey, Some(TraceState(parentSpan, parentOwner, traceStatePairs)))
    }

    contextWithTraceState getOrElse context
  }

  private def packPairs(pairs: Seq[(String, String)]): String =
    pairs.map { case (key, value) => key + "=" + value } mkString (",")
}

object W3CTraceContext {
  val OwnSystemName = "kamon"
  val TraceParentHeaderName = "traceparent"
  val TraceStateHeaderName = "tracestate"
  val TraceStateKey = Key.broadcast[Option[TraceState]]("tracestate", None)

  case class TraceState(
    parent:      Span,
    parentOwner: String,
    state:       Seq[(String, String)]
  )

  def encodeTraceParent(parent: Span): String = {
    val spanContext = parent.context()
    val samplingDecision = if (spanContext.samplingDecision == SamplingDecision.Sample) "01" else "00"

    s"00-${spanContext.traceID.string}-${spanContext.spanID.string}-${samplingDecision}"
  }

  def decodeTraceParent(traceParent: String, identityProvider: IdentityProvider): Option[Span] = {
    def unpackSamplingDecision(decision: String): SamplingDecision =
      if ("01" == decision) SamplingDecision.Sample else SamplingDecision.Unknown

    val traceParentComponents = traceParent.split("\\-")
    if (traceParentComponents.length != 4) None else {
      val remoteSpanContext = SpanContext(
        traceID = identityProvider.traceIdGenerator.from(traceParentComponents(1)),
        spanID = identityProvider.spanIdGenerator.from(traceParentComponents(2)),
        parentID = IdentityProvider.NoIdentifier,
        samplingDecision = unpackSamplingDecision(traceParentComponents(3))
      )

      if (remoteSpanContext.traceID == IdentityProvider.NoIdentifier || remoteSpanContext.spanID == IdentityProvider.NoIdentifier)
        None
      else
        Some(Span.Remote(remoteSpanContext))
    }
  }

  def decodeTraceState(traceState: String): Option[Seq[(String, String)]] = {
    if (traceState.length == 0) None else {
      val stateComponents = traceState.split("\\,")
      val unpackedTraceState = stateComponents.map(state => {
        val separatorIndex = state.indexOf('=')
        (state.substring(0, separatorIndex), state.substring(separatorIndex + 1))
      })

      Some(unpackedTraceState)
    }
  }
}
