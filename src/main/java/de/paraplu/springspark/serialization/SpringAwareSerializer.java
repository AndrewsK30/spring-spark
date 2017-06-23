package de.paraplu.springspark.serialization;

import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.SerializerInstance;

public class SpringAwareSerializer extends KryoSerializer {
	/**
	 * The serial version UID
	 */
	private static final long serialVersionUID = 1L;
	private boolean useUnsafe = false;

	public SpringAwareSerializer(SparkConf conf) {
		super(conf);
		this.useUnsafe = conf.getBoolean("spark.serializer.SpringAwareSerializer.useUnsafe", false);
	}

	@Override
	public SerializerInstance newInstance() {
		return new SpringAwareSerializerInstance(this, useUnsafe);
	}

}
