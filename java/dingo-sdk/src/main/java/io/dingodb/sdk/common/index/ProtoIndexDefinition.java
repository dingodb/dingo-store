package io.dingodb.sdk.common.index;

import io.dingodb.meta.Meta;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.SDKCommonId;
import io.dingodb.sdk.common.codec.DingoKeyValueCodec;
import io.dingodb.sdk.common.partition.Partition;
import io.dingodb.sdk.common.partition.ProtoPartitionRule;
import io.dingodb.sdk.common.serial.schema.LongSchema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.Collections;
import java.util.List;

import static io.dingodb.sdk.common.utils.EntityConversion.mapping;

@ToString(of = {"id", "definition"})
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class ProtoIndexDefinition implements Index {

    private static final LongSchema SCHEMA = new LongSchema(0);
    private static final DingoKeyValueCodec CODEC = new DingoKeyValueCodec(0, Collections.singletonList(SCHEMA));
    private static final List<String> PKS = Collections.singletonList("ID");

    static {
        SCHEMA.setIsKey(true);
        SCHEMA.setAllowNull(false);
    }

    @Getter
    @EqualsAndHashCode.Include
    private final DingoCommonId id;

    @Getter
    @EqualsAndHashCode.Include
    private final Meta.IndexDefinition definition;

    private ProtoIndexDefinition(DingoCommonId id, Meta.IndexDefinition definition) {
        this.id = id;
        this.definition = definition;
    }

    @Override
    public String getName() {
        return this.definition.getName().toUpperCase();
    }

    @Override
    public Integer getVersion() {
        return this.definition.getVersion();
    }

    @Override
    public Partition getIndexPartition() {
        return ProtoPartitionRule.from(this.definition.getIndexPartition(), CODEC);
    }

    @Override
    public Integer getReplica() {
        return this.definition.getReplica();
    }

    @Override
    public IndexParameter getIndexParameter() {
        return mapping(this.definition.getIndexParameter());
    }

    @Override
    public Boolean getIsAutoIncrement() {
        return this.definition.getWithAutoIncrment();
    }

    @Override
    public Long getAutoIncrement() {
        return this.definition.getAutoIncrement();
    }

    public static ProtoIndexDefinition from(DingoCommonId id, Meta.IndexDefinition definition) {
        return new ProtoIndexDefinition(id, definition);


    }

    public static ProtoIndexDefinition from(Meta.IndexDefinitionWithId from) {
        return new ProtoIndexDefinition(SDKCommonId.from(from.getIndexId()), from.getIndexDefinition());
    }

    public static ProtoIndexDefinition copy(Index index) {
        if (index instanceof ProtoIndexDefinition) {
            return new ProtoIndexDefinition(index.id(), ((ProtoIndexDefinition) index).definition);
        }

        Meta.IndexDefinition.Builder builder = Meta.IndexDefinition.newBuilder();
        builder
            .setName(index.getName())
            .setVersion(index.getVersion())
            .setIndexPartition(ProtoPartitionRule.copy(index.getIndexPartition(), CODEC).getDelegate())
            .setReplica(index.getReplica())
            .setIndexParameter(mapping(index.getIndexParameter()))
            .setWithAutoIncrment(index.getIsAutoIncrement())
            .setAutoIncrement(index.getAutoIncrement());

        return new ProtoIndexDefinition(index.id(), builder.build());
    }

    public static ProtoIndexDefinition build(Meta.TableIdWithPartIds ids, Index index) {

        Meta.IndexDefinition.Builder builder = Meta.IndexDefinition.newBuilder();
        Partition partition = index.getIndexPartition();
        builder
            .setName(index.getName().toUpperCase())
            .setVersion(index.getVersion())
            .setIndexPartition(
                ProtoPartitionRule.build(
                    ids.getPartIdsList(), partition.getStrategy(), PKS, partition.getDetails(), CODEC
                ).getDelegate()
            )
            .setReplica(index.getReplica())
            .setIndexParameter(mapping(index.getIndexParameter()))
            .setWithAutoIncrment(index.getIsAutoIncrement())
            .setAutoIncrement(index.getAutoIncrement());

        return new ProtoIndexDefinition(SDKCommonId.from(ids.getTableId()), builder.build());
    }

}
