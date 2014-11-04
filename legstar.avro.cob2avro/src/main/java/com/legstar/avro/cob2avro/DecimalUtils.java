package com.legstar.avro.cob2avro;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public class DecimalUtils {
	
	private DecimalUtils() {}
	
	public static BigDecimal toBigDecimal(ByteBuffer avroDecimal, int scale) {
		return new BigDecimal(new BigInteger(avroDecimal.array()), scale);
	}
	
	public static ByteBuffer toByteBuffer(BigDecimal decimal) {
		return ByteBuffer.wrap(decimal.unscaledValue().toByteArray());
	}

}
