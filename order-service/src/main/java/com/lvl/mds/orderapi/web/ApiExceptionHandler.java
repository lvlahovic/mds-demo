package com.lvl.mds.orderapi.web;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.ErrorResponse;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Turns every error this API can produce into an RFC 7807
 * {@code application/problem+json} body, replacing Spring's default
 * per-exception shapes - a bare field-errors array for {@code @Valid}
 * failures, a whitelabel page or raw stack trace for anything unhandled -
 * with one consistent contract.
 *
 * <p>Extending {@link ResponseEntityExceptionHandler} rather than writing
 * every {@code @ExceptionHandler} from scratch is deliberate: that base
 * class already turns the ~20 exceptions Spring MVC itself can raise
 * (malformed JSON, wrong HTTP method, unsupported media type, ...) into
 * {@link ProblemDetail} bodies - including
 * {@link org.springframework.web.server.ResponseStatusException}, which is
 * how {@link OrdersController} and {@link OrderStatusStream} already report
 * a missing order (404) or a duplicate {@code orderId} (409). This class
 * only adds what those defaults don't cover: listing individual field
 * errors for validation failures, tagging every response with a stable
 * {@code type} URI so a client can branch on the failure kind instead of
 * parsing {@code detail}, telling a known dependency outage (Redis down
 * while publishing, see {@link #handleBrokerUnavailable}) apart from a
 * genuine bug (see {@link #handleUnexpected}) - the former is 503 and worth
 * retrying, the latter is 500 and isn't - so that every error this API can
 * produce comes back as {@code application/problem+json} instead of a stack
 * trace, and a client can tell them apart without parsing English.
 */
@RestControllerAdvice
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {

	private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

	/**
	 * Base for every {@code type} URI this handler sets. Not meant to be
	 * dereferenced - it's just a stable namespace a client can match against,
	 * one path segment per HTTP status (see {@link #slug}).
	 */
	private static final String PROBLEM_TYPE_BASE = "https://order-service.mds-demo/problems/";

	/**
	 * Lists each rejected field individually under the {@code errors}
	 * extension member, instead of the superclass's default single generic
	 * "Invalid request content." detail - a client (or a human reading the
	 * response while debugging) sees exactly which field failed and why,
	 * without parsing a message string.
	 */
	@Override
	protected ResponseEntity<Object> handleMethodArgumentNotValid(
			MethodArgumentNotValidException ex, HttpHeaders headers, HttpStatusCode status, WebRequest request) {

		Map<String, String> fieldErrors = new LinkedHashMap<>();
		for (FieldError error : ex.getBindingResult().getFieldErrors()) {
			fieldErrors.put(error.getField(), error.getDefaultMessage());
		}

		ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, "Request body failed validation");
		problem.setTitle("Validation failed");
		problem.setProperty("errors", fieldErrors);

		return handleExceptionInternal(ex, problem, headers, status, request);
	}

	/**
	 * The one call in this API that talks to an external system
	 * synchronously: {@code POST /orders} publishes to {@code orders-stream}
	 * via {@link com.lvl.mds.orderapi.messaging.producers.OrderEventPublisher}
	 * before returning. When Redis is down or times out, that publish throws
	 * a {@link DataAccessException} (Spring Data's common root for
	 * {@code RedisConnectionFailureException}, {@code QueryTimeoutException},
	 * {@code RedisSystemException}, ...). That is a dependency being
	 * unavailable, not a bug in this service, so it is reported as 503 with a
	 * {@code Retry-After} hint rather than falling into
	 * {@link #handleUnexpected}'s generic 500 - a client can tell "the broker
	 * is down, retry" apart from "something here is broken". The order itself
	 * is left behind as {@code CREATED} (see
	 * {@link com.lvl.mds.orderapi.services.OrderService#createOrder}), which
	 * is what makes retrying safe: nothing was published, so nothing is
	 * duplicated.
	 */
	@ExceptionHandler(DataAccessException.class)
	public ResponseEntity<Object> handleBrokerUnavailable(DataAccessException ex, WebRequest request) {
		log.error("Broker unavailable while handling {}", request.getDescription(false), ex);

		ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE,
				"Order broker is temporarily unavailable - try again shortly");
		problem.setTitle("Service Unavailable");

		HttpHeaders headers = new HttpHeaders();
		headers.add(HttpHeaders.RETRY_AFTER, "5");

		return handleExceptionInternal(ex, problem, headers, HttpStatus.SERVICE_UNAVAILABLE, request);
	}

	/**
	 * Catches whatever the superclass's own exception list - and
	 * {@link #handleBrokerUnavailable} above - doesn't: a genuine,
	 * unanticipated failure rather than a request-shape problem Spring MVC
	 * already recognizes or a known dependency outage. The client only ever
	 * sees a generic message; the real exception, with its stack trace, goes
	 * to the log, since {@code detail} is client-facing and must not leak
	 * internals.
	 */
	@ExceptionHandler(Exception.class)
	public ResponseEntity<Object> handleUnexpected(Exception ex, WebRequest request) {
		log.error("Unhandled exception for {}", request.getDescription(false), ex);

		ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.INTERNAL_SERVER_ERROR,
				"An unexpected error occurred");
		problem.setTitle("Internal Server Error");

		return handleExceptionInternal(ex, problem, new HttpHeaders(), HttpStatus.INTERNAL_SERVER_ERROR, request);
	}

	/**
	 * Single choke point every handler above - and every one inherited from
	 * {@link ResponseEntityExceptionHandler}, including the default handling
	 * of {@code ResponseStatusException} that backs this API's 404s and 409 -
	 * funnels through. That makes it the one place to attach a stable,
	 * per-status {@code type} URI uniformly, rather than repeating the same
	 * line in every handler method.
	 *
	 * <p>The superclass's own handlers (e.g. {@code handleErrorResponseException},
	 * which is what actually handles {@code ResponseStatusException}) call this
	 * with a {@code null} body and let it resolve to {@link ErrorResponse#getBody()}
	 * internally - so the body has to be resolved the same way here too, before
	 * the {@code type} can be attached. It is the same {@link ProblemDetail}
	 * instance either way, so mutating it here is visible to the response the
	 * superclass ultimately writes. Never overwrites a {@code type} a handler
	 * set explicitly.
	 */
	@Override
	protected ResponseEntity<Object> handleExceptionInternal(
			Exception ex, Object body, HttpHeaders headers, HttpStatusCode statusCode, WebRequest request) {

		ProblemDetail problem = body instanceof ProblemDetail pd ? pd
				: body == null && ex instanceof ErrorResponse errorResponse ? errorResponse.getBody() : null;

		if (problem != null && problem.getType() == null) {
			problem.setType(URI.create(PROBLEM_TYPE_BASE + slug(statusCode)));
		}
		return super.handleExceptionInternal(ex, body, headers, statusCode, request);
	}

	private static String slug(HttpStatusCode statusCode) {
		HttpStatus status = HttpStatus.resolve(statusCode.value());
		String reasonPhrase = status != null ? status.getReasonPhrase() : "error";
		return reasonPhrase.toLowerCase(Locale.ROOT).replace(' ', '-');
	}
}
