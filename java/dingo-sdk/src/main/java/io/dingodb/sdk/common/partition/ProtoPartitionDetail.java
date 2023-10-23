package io.dingodb.sdk.common.partition;

import com.google.protobuf.ByteString;
import io.dingodb.common.Common;
import io.dingodb.meta.Meta;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.SDKCommonId;
import io.dingodb.sdk.common.codec.DingoKeyValueCodec;
import io.dingodb.sdk.common.serial.Config;
import io.dingodb.sdk.common.utils.EntityConversion;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Delegate;

import java.util.Arrays;
import java.util.Objects;

@ToString
public class ProtoPartitionDetail implements PartitionDetail {

    protected static final ProtoPartitionDetail EMPTY = new ProtoPartitionDetail(null, null);

    private static final byte[] PREFIX_ONLY_KEY = new byte[Config.KEY_PREFIX_SIZE];

    @Getter
    @Delegate
    @ToString.Include
    private final Meta.Partition delegate;

    @Setter
    @Getter
    private DingoKeyValueCodec codec;

    public ProtoPartitionDetail(Meta.Partition delegate, DingoKeyValueCodec codec) {
        this.delegate = delegate;
        this.codec = codec;
    }

    @Override
    public DingoCommonId id() {
        return SDKCommonId.from(delegate.getId());
    }

    @Override
    public String getPartName() {
        return delegate.getId().toString();
    }

    @Override
    public String getOperator() {
        return null;
    }

    @Override
    public Object[] getOperand() {
        if (delegate == null) {
            return new Object[0];
        }
        return codec
            .decodeKeyPrefix(codec.resetPrefix(delegate.getRange().getStartKey().toByteArray()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof PartitionDetail) {
            PartitionDetail other = (PartitionDetail) o;
            if (id() != null && other.id() != null && !id().equals(other.id())) {
                return false;
            }
            return delegate.equals(copy(other, codec).delegate);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id(), Arrays.hashCode(getOperand()));
    }

    public static ProtoPartitionDetail from(Meta.Partition from) {
        return new ProtoPartitionDetail(from, null);
    }

    public static ProtoPartitionDetail from(Meta.Partition from, DingoKeyValueCodec codec) {
        return new ProtoPartitionDetail(from, codec);
    }

    public static ProtoPartitionDetail copy(PartitionDetail copy, DingoKeyValueCodec codec) {
        if (copy instanceof ProtoPartitionDetail) {
            return from(((ProtoPartitionDetail) copy).delegate, codec);
        }
        return build(copy.id(), copy, codec);
    }

    public static ProtoPartitionDetail build(DingoCommonId id, PartitionDetail copy, DingoKeyValueCodec codec) {
        Meta.Partition.Builder builder = Meta.Partition.newBuilder();

        Object[] operand = copy.getOperand();

        builder.setId(EntityConversion.mapping(id));

        builder.setRange(
            Common.Range.newBuilder()
                .setStartKey(ByteString.copyFrom(codec.resetPrefix(codec.encodeKeyPrefix(operand), id.entityId())))
                .setEndKey(ByteString.copyFrom(codec.resetPrefix(copyPrefixOnlyKey(), id.entityId() + 1)))
                .build()
        );

        return from(builder.build(), codec);
    }

    private static byte[] copyPrefixOnlyKey() {
        return Arrays.copyOf(PREFIX_ONLY_KEY, PREFIX_ONLY_KEY.length);
    }

}
