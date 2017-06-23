package de.paraplu.springspark.serialization;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.spark.serializer.DeserializationStream;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.KryoSerializerInstance;
import org.apache.spark.serializer.SerializationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.paraplu.springspark.util.SpringBuilder;
import scala.reflect.ClassTag;

public class SpringAwareSerializerInstance extends KryoSerializerInstance {
	private static final Logger LOG = LoggerFactory.getLogger(SpringAwareSerializerInstance.class);

	public SpringAwareSerializerInstance(KryoSerializer ks, boolean useUnsafe) {
		super(ks, useUnsafe);
	}

	@Override
	public <T> T deserialize(ByteBuffer bytes, ClassLoader loader, ClassTag<T> classTag) {
		final T deserialized = super.deserialize(bytes, loader, classTag);
		// autowire dependencies
		LOG.debug("deserialized object {} class tag {}", deserialized, classTag);
		SpringBuilder.autowire(deserialized);
		return deserialized;
	}

	@Override
	public <T> T deserialize(ByteBuffer bytes, ClassTag<T> classTag) {
		final T deserialized = super.deserialize(bytes, classTag);
		// autowire dependencies
		LOG.debug("deserialized object {} class tag {}", deserialized, classTag);
		SpringBuilder.autowire(deserialized);
		return deserialized;
	}

	@Override
	public DeserializationStream deserializeStream(InputStream s) {
		return new SpringAwareDeserializationStream(super.deserializeStream(s));
	}

	@Override
	public <T> ByteBuffer serialize(T t, ClassTag<T> classTag) {
		return super.serialize(t, classTag);
	}

	@Override
	public SerializationStream serializeStream(OutputStream s) {
		return super.serializeStream(s);
	}

}
