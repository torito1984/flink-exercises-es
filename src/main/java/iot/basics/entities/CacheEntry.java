package iot.basics.entities;

import lombok.Data;
import org.apache.flink.api.common.time.Time;

@Data
public class CacheEntry<T> {

	private final long timestamp;
	private final T cached;

	public boolean isExpired(Time timeout) {
		return System.currentTimeMillis() - timeout.toMilliseconds() >= timestamp;
	}

}
