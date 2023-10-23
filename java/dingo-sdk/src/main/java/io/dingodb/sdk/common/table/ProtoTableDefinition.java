package io.dingodb.sdk.common.table;

import io.dingodb.common.Common;
import io.dingodb.meta.Meta;
import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.SDKCommonId;
import io.dingodb.sdk.common.codec.DingoKeyValueCodec;
import io.dingodb.sdk.common.index.IndexParameter;
import io.dingodb.sdk.common.partition.Partition;
import io.dingodb.sdk.common.partition.ProtoPartitionRule;
import io.dingodb.sdk.common.utils.EntityConversion;
import io.dingodb.sdk.common.utils.Optional;
import io.dingodb.sdk.common.utils.Parameters;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@ToString(onlyExplicitlyIncluded = true)
public class ProtoTableDefinition implements Table {

    @Getter
    @ToString.Include
    private final DingoCommonId id;

    @Getter
    @ToString.Include
    private final Meta.TableDefinition definition;

    public ProtoTableDefinition(Meta.TableDefinitionWithId definition) {
        this.id = SDKCommonId.from(definition.getTableId());
        this.definition = definition.getTableDefinition();
    }

    public ProtoTableDefinition(DingoCommonId id, Meta.TableDefinition definition) {
        this.id = id;
        this.definition = definition;
    }

    @Override
    public DingoCommonId id() {
        return id;
    }

    public String getName() {
        return definition.getName().toUpperCase();
    }

    @Override
    public List<Column> getColumns() {
        return definition.getColumnsList().stream().map(ProtoColumnDefinition::from).collect(Collectors.toList());
    }

    @Override
    public Partition getPartition() {
        return ProtoPartitionRule.from(definition.getTablePartition(), DingoKeyValueCodec.of(this));
    }

    @Override
    public int getVersion() {
        return definition.getVersion();
    }

    @Override
    public int getTtl() {
        return (int) definition.getTtl();
    }

    @Override
    public String getEngine() {
        return definition.getEngine().name();
    }

    @Override
    public Map<String, String> getProperties() {
        return definition.getPropertiesMap();
    }

    @Override
    public int getReplica() {
        return definition.getReplica();
    }

    @Override
    public long getAutoIncrement() {
        return definition.getAutoIncrement();
    }

    @Override
    public String getCreateSql() {
        return definition.getCreateSql();
    }

    @Override
    public IndexParameter getIndexParameter() {
        return EntityConversion.mapping(definition.getIndexParameter());
    }

    public static ProtoTableDefinition from(Meta.TableDefinitionWithId from) {
        return new ProtoTableDefinition(from);
    }

    public static ProtoTableDefinition copy(Table copy) {
        if (copy instanceof ProtoTableDefinition) {
            return new ProtoTableDefinition(((ProtoTableDefinition) copy).id, ((ProtoTableDefinition) copy).definition);
        }
        return new ProtoTableDefinition(
            copy.id(),
            Meta.TableDefinition.newBuilder()
                .setName(copy.getName())
                .setVersion(copy.getVersion())
                .setTtl(copy.getTtl())
                .setTablePartition(
                    ProtoPartitionRule.copy(copy.getPartition(), DingoKeyValueCodec.of(copy)).getDelegate()
                )
                .setEngine(Common.Engine.valueOf(copy.getEngine()))
                .setReplica(copy.getReplica())
                .addAllColumns(
                    copy.getColumns().stream()
                        .map(c -> ProtoColumnDefinition.copy(c).getDelegate()).collect(Collectors.toList())
                )
                .setAutoIncrement(copy.getAutoIncrement())
                .putAllProperties(copy.getProperties())
                .setCreateSql(copy.getCreateSql())
                .setIndexParameter(Optional.mapOrGet(
                    copy.getIndexParameter(),
                    EntityConversion::mapping,
                    () -> Common.IndexParameter.newBuilder().build()))
                .build()
        );
    }

    public static ProtoTableDefinition build(Meta.TableIdWithPartIds ids, Table copy) {
        Optional.ofNullable(copy.getColumns())
            .filter(__ -> __.stream()
                .map(Column::getName)
                .distinct()
                .count() == __.size())
            .orElseThrow(() -> new DingoClientException("Table field names cannot be repeated."));

        Partition partition = copy.getPartition();
        List<Column> keyColumns = copy.getKeyColumns();
        keyColumns.sort(Comparator.comparingInt(Column::getPrimary));
        return new ProtoTableDefinition(
            SDKCommonId.from(ids.getTableId()),
            Meta.TableDefinition.newBuilder()
                .setName(copy.getName().toUpperCase())
                .setVersion(copy.getVersion())
                .setTtl(copy.getTtl())
                .setTablePartition(
                    ProtoPartitionRule.build(
                        ids.getPartIdsList(),
                        Optional.mapOrGet(partition, Partition::getStrategy, () -> "RANGE"),
                        keyColumns.stream().map(Column::getName).collect(Collectors.toList()),
                        Optional.mapOrGet(partition, Partition::getDetails, Collections::emptyList),
                        DingoKeyValueCodec.of(ids.getTableId().getEntityId(), keyColumns)
                    ).getDelegate()
                )
                .setEngine(Common.Engine.valueOf(copy.getEngine()))
                .setReplica(copy.getReplica())
                .addAllColumns(
                    copy.getColumns().stream()
                        .map(c -> ProtoColumnDefinition.copy(c).getDelegate()).collect(Collectors.toList())
                )
                .setAutoIncrement(copy.getAutoIncrement())
                .putAllProperties(copy.getProperties() == null ? new HashMap<>() : copy.getProperties())
                .setCreateSql(Parameters.cleanNull(copy.getCreateSql(), ""))
                .setIndexParameter(Optional.mapOrGet(
                    copy.getIndexParameter(),
                    EntityConversion::mapping,
                    () -> Common.IndexParameter.newBuilder().build()))
                .build()
        );
    }

}
