package io.dingodb.sdk.common.partition;

import io.dingodb.meta.Meta;
import io.dingodb.sdk.common.SDKCommonId;
import io.dingodb.sdk.common.codec.DingoKeyValueCodec;
import io.dingodb.sdk.common.utils.Optional;
import io.dingodb.sdk.common.utils.Parameters;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.dingodb.sdk.common.utils.ByteArrayUtils.compare;

@ToString
public class ProtoPartitionRule implements Partition {

    @Getter
    @ToString.Include
    private final Meta.PartitionRule delegate;

    @Getter
    @Setter
    private DingoKeyValueCodec codec;

    public ProtoPartitionRule(Meta.PartitionRule delegate, DingoKeyValueCodec codec) {
        this.delegate = delegate;
        this.codec = codec;
    }

    @Override
    public String getFuncName() {
        return getStrategy();
    }

    @Override
    public String getStrategy() {
        return getStrategy(delegate.getStrategy());
    }

    @Override
    public List<String> getCols() {
        return delegate.getColumnsList();
    }

    @Override
    public List<PartitionDetail> getDetails() {
        // skip first?
        return delegate.getPartitionsList().stream()
            .map(detail -> ProtoPartitionDetail.from(detail, codec))
            .sorted((_1, _2) ->
                compare(_1.getRange().getStartKey().toByteArray(), _2.getRange().getStartKey().toByteArray())
            )
            .collect(Collectors.toList());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof Partition) {
            return delegate.equals(copy((Partition) o, codec).delegate);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getStrategy(), getCols(), getDetails());
    }

    public static ProtoPartitionRule from(Meta.PartitionRule from) {
        return new ProtoPartitionRule(from, null);
    }

    public static ProtoPartitionRule from(Meta.PartitionRule from, DingoKeyValueCodec codec) {
        return new ProtoPartitionRule(from, codec);
    }

    public static ProtoPartitionRule copy(Partition copy, DingoKeyValueCodec codec) {
        if (copy instanceof ProtoPartitionRule) {
            return from(((ProtoPartitionRule) copy).delegate, codec);
        }
        Meta.PartitionRule.Builder builder = Meta.PartitionRule.newBuilder();

        builder.addAllColumns(copy.getCols());
        builder.setStrategy(getStrategy(Parameters.cleanNull(copy.getStrategy(), "RANGE")));
        copy.getDetails().stream()
            .map(detail -> ProtoPartitionDetail.copy(detail, codec).getDelegate())
            .forEach(builder::addPartitions);

        return new ProtoPartitionRule(builder.build(), codec);
    }

    public static ProtoPartitionRule build(
        List<Meta.DingoCommonId> ids,
        String strategy,
        List<String> cols,
        List<PartitionDetail> details,
        DingoKeyValueCodec codec
    ) {
        Meta.PartitionRule.Builder builder = Meta.PartitionRule.newBuilder();

        builder.setStrategy(getStrategy(Parameters.cleanNull(strategy, "RANGE")));
        builder.addAllColumns(cols);

        details = Optional.ofNullable(details)
            .filter(Objects::nonNull)
            .filterNot(List::isEmpty)
            .ifAbsentSet(() -> Collections.singletonList(ProtoPartitionDetail.EMPTY))
            .get();

        int idIndex = 0;
        if (details.stream().map(PartitionDetail::getOperand).noneMatch(__ -> __.length == 0)) {
            builder.addPartitions(ProtoPartitionDetail.build(
                SDKCommonId.from(ids.get(idIndex++)), ProtoPartitionDetail.EMPTY, codec).getDelegate()
            );
        }

        for (PartitionDetail detail : details) {
            builder.addPartitions(
                ProtoPartitionDetail.build(SDKCommonId.from(ids.get(idIndex++)), detail, codec).getDelegate()
            );
        }

        return new ProtoPartitionRule(builder.build(), codec);
    }

    private static String getStrategy(Meta.PartitionStrategy partitionStrategy) {
        switch (partitionStrategy) {
            case PT_STRATEGY_RANGE:
                return "RANGE";
            case PT_STRATEGY_HASH:
                return "HASH";
            case UNRECOGNIZED:
            default:
                throw new IllegalStateException("Unexpected value: " + partitionStrategy);
        }
    }

    private static Meta.PartitionStrategy getStrategy(String strategy) {
        switch (strategy.toUpperCase()) {
            case "HASH":
                return Meta.PartitionStrategy.PT_STRATEGY_HASH;

            case "RANGE":
            default:
                return Meta.PartitionStrategy.PT_STRATEGY_RANGE;
        }
    }

}
