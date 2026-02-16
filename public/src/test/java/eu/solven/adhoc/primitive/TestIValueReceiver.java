package eu.solven.adhoc.primitive;

import java.util.ArrayList;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIValueReceiver {
	@Test
	public void testOnPrimitives() {
		List<Object> list = new ArrayList<>();

		IValueReceiver receiver = new IValueReceiver() {

			@Override
			public void onObject(Object v) {
				list.add(v);
			}
		};

		receiver.onObject("foo");
		receiver.onLong(123);
		receiver.onDouble(12.34);

		Assertions.assertThat(list).containsExactly("foo", 123L, 12.34D);
	}
}
