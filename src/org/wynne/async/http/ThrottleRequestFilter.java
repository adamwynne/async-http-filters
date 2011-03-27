package org.wynne.async.http;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import com.ning.http.client.AsyncHandler;
import com.ning.http.client.HttpResponseBodyPart;
import com.ning.http.client.HttpResponseHeaders;
import com.ning.http.client.HttpResponseStatus;
import com.ning.http.client.filter.*;

public class ThrottleRequestFilter implements RequestFilter {

	private final int maxConnections;
	private final Semaphore available;
	private final int maxWait;

	public ThrottleRequestFilter(int maxConnections) {
		this.maxConnections = maxConnections;
		this.maxWait = Integer.MAX_VALUE;
		available = new Semaphore(maxConnections, true);
	}
	
	public ThrottleRequestFilter(int maxConnections, int maxWait) {
		this.maxConnections = maxConnections;
		this.maxWait = maxWait;
		available = new Semaphore(maxConnections, true);
	}
	
	@SuppressWarnings({ "deprecation", "rawtypes", "unchecked" })
	public FilterContext filter(FilterContext ctx) throws
	FilterException {
		try {
			if (!available.tryAcquire(maxWait, TimeUnit.MILLISECONDS))
			{
				throw new FilterException(
						String.format("No slot available for Request %s with AsyncHandler %s",
								ctx.getRequest(), ctx.getAsyncHandler()));
			};
		} catch (InterruptedException e) {
			throw new FilterException(
					String.format("Interrupted Request %s with AsyncHandler %s",
							ctx.getRequest(), ctx.getAsyncHandler()));
		}
		return new FilterContext(
				new AsyncHandlerWrapper(ctx.getAsyncHandler()),
				ctx.getRequest());
	}
	
	public int getMaxConnections() {
		return maxConnections;
	}

	private class AsyncHandlerWrapper<T> implements AsyncHandler<T> {
		
		private final AsyncHandler<T> asyncHandler;
		
		public AsyncHandlerWrapper(AsyncHandler<T> asyncHandler) {
			this.asyncHandler = asyncHandler;
		}
		
		public void onThrowable(Throwable t) {
			available.release();
			asyncHandler.onThrowable(t);
		}
		
		public STATE onBodyPartReceived(HttpResponseBodyPart bodyPart)
		throws Exception {
			return asyncHandler.onBodyPartReceived(bodyPart);
		}
		
		public STATE onStatusReceived(HttpResponseStatus
				responseStatus)
		throws Exception {
			return asyncHandler.onStatusReceived(responseStatus);
		}
		
		public STATE onHeadersReceived(HttpResponseHeaders headers)
		throws Exception {
			return asyncHandler.onHeadersReceived(headers);
		}
		
		public T onCompleted() throws Exception {
			available.release();
			return asyncHandler.onCompleted();
		}
	}
}