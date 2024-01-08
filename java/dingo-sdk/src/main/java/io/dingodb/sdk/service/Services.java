package io.dingodb.sdk.service;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.dingodb.sdk.common.codec.CodecUtils;
import io.dingodb.sdk.common.utils.ByteArrayUtils;
import io.dingodb.sdk.common.utils.Optional;
import io.dingodb.sdk.common.utils.Parameters;
import io.dingodb.sdk.service.caller.ServiceCaller;
import io.dingodb.sdk.service.entity.common.Location;
import io.dingodb.sdk.service.entity.common.Range;
import io.dingodb.sdk.service.entity.coordinator.GetCoordinatorMapResponse;
import io.dingodb.sdk.service.entity.coordinator.ScanRegionInfo;
import io.dingodb.sdk.service.entity.coordinator.ScanRegionsRequest;
import io.dingodb.sdk.service.entity.coordinator.ScanRegionsResponse;
import io.dingodb.sdk.service.entity.meta.DingoCommonId;
import lombok.SneakyThrows;

import java.util.Arrays;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.dingodb.sdk.common.utils.ByteArrayUtils.compare;
import static io.grpc.CallOptions.DEFAULT;

public class Services {

    public static final int DEFAULT_RETRY_TIMES = 30;

    private static final LoadingCache<Set<Location>, CoordinatorChannelProvider> coordinatorCache = CacheBuilder
        .newBuilder()
        .expireAfterAccess(60, TimeUnit.MINUTES)
        .expireAfterWrite(60, TimeUnit.MINUTES)
        .maximumSize(8)
        .build(new CacheLoader<Set<Location>, CoordinatorChannelProvider>() {
            @Override
            public CoordinatorChannelProvider load(Set<Location> key) {
                return coordinatorServiceChannelProvider(key);
            }
        });

    private static final LoadingCache<Set<Location>, CoordinatorChannelProvider> autoIncrementCache = CacheBuilder
        .newBuilder()
        .expireAfterAccess(60, TimeUnit.MINUTES)
        .expireAfterWrite(60, TimeUnit.MINUTES)
        .maximumSize(8)
        .build(new CacheLoader<Set<Location>, CoordinatorChannelProvider>() {
            @Override
            public CoordinatorChannelProvider load(Set<Location> key) {
                return autoIncrementChannelProvider(key);
            }
        });

    private static final LoadingCache<Set<Location>, CoordinatorChannelProvider> metaCache = CacheBuilder
        .newBuilder()
        .expireAfterAccess(60, TimeUnit.MINUTES)
        .expireAfterWrite(60, TimeUnit.MINUTES)
        .maximumSize(8)
        .build(new CacheLoader<Set<Location>, CoordinatorChannelProvider>() {
            @Override
            public CoordinatorChannelProvider load(Set<Location> key) {
                return metaServiceChannelProvider(key);
            }
        });

    private static final LoadingCache<Set<Location>, CoordinatorChannelProvider> tsoCache = CacheBuilder
        .newBuilder()
        .expireAfterAccess(60, TimeUnit.MINUTES)
        .expireAfterWrite(60, TimeUnit.MINUTES)
        .maximumSize(8)
        .build(new CacheLoader<Set<Location>, CoordinatorChannelProvider>() {
            @Override
            public CoordinatorChannelProvider load(Set<Location> key) {
                return tsoServiceChannelProvider(key);
            }
        });

    private static final LoadingCache<Set<Location>, CoordinatorChannelProvider> versionCache = CacheBuilder
        .newBuilder()
        .expireAfterAccess(60, TimeUnit.MINUTES)
        .expireAfterWrite(60, TimeUnit.MINUTES)
        .maximumSize(8)
        .build(new CacheLoader<Set<Location>, CoordinatorChannelProvider>() {
            @Override
            public CoordinatorChannelProvider load(Set<Location> key) {
                return kvServiceChannelProvider(key);
            }
        });

    private static final LoadingCache<Set<Location>, LoadingCache<Long, RegionChannelProvider>> regionCache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(30, TimeUnit.MINUTES)
            .expireAfterWrite(30, TimeUnit.MINUTES)
            .maximumSize(64)
            .build(new CacheLoader<Set<Location>, LoadingCache<Long, RegionChannelProvider>>() {
                @Override
                public LoadingCache<Long, RegionChannelProvider> load(Set<Location> locations) throws Exception {
                    return CacheBuilder.newBuilder()
                        .expireAfterAccess(10, TimeUnit.MINUTES)
                        .expireAfterWrite(10, TimeUnit.MINUTES)
                        .maximumSize(4096)
                        .build(new CacheLoader<Long, RegionChannelProvider>() {
                            @Override
                            public RegionChannelProvider load(Long regionId) throws Exception {
                                return new RegionChannelProvider(coordinatorService(locations), regionId);
                            }
                        });
                }
            });

    private static final LoadingCache<Set<Location>, LoadingCache<DingoCommonId, TableRegionsFailOver>> tableFailOverCache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(30, TimeUnit.MINUTES)
            .expireAfterWrite(30, TimeUnit.MINUTES)
            .maximumSize(64)
            .build(new CacheLoader<Set<Location>, LoadingCache<DingoCommonId, TableRegionsFailOver>>() {
                @Override
                public LoadingCache<DingoCommonId, TableRegionsFailOver> load(Set<Location> locations) throws Exception {
                    return CacheBuilder.newBuilder()
                        .expireAfterAccess(10, TimeUnit.MINUTES)
                        .expireAfterWrite(10, TimeUnit.MINUTES)
                        .maximumSize(256)
                        .build(new CacheLoader<DingoCommonId, TableRegionsFailOver>() {
                            @Override
                            public TableRegionsFailOver load(DingoCommonId tableId) throws Exception {
                                return new TableRegionsFailOver(tableId, metaService(locations));
                            }
                        });
                }
            });

    private static final NavigableMap<byte[], NavigableMap<byte[], ScanRegionInfo>> rangeRegions =
        new ConcurrentSkipListMap<>(ByteArrayUtils::compare);

    public static CoordinatorChannelProvider coordinatorServiceChannelProvider(Set<Location> locations) {
        return new CoordinatorChannelProvider(locations, GetCoordinatorMapResponse::getLeaderLocation);
    }

    public static CoordinatorChannelProvider metaServiceChannelProvider(Set<Location> locations) {
        return new CoordinatorChannelProvider(locations, GetCoordinatorMapResponse::getLeaderLocation);
    }

    public static CoordinatorChannelProvider tsoServiceChannelProvider(Set<Location> locations) {
        return new CoordinatorChannelProvider(locations, GetCoordinatorMapResponse::getTsoLeaderLocation);
    }

    public static CoordinatorChannelProvider kvServiceChannelProvider(Set<Location> locations) {
        return new CoordinatorChannelProvider(locations, GetCoordinatorMapResponse::getKvLeaderLocation);
    }

    public static CoordinatorChannelProvider autoIncrementChannelProvider(Set<Location> locations) {
        return new CoordinatorChannelProvider(locations, GetCoordinatorMapResponse::getAutoIncrementLeaderLocation);
    }
    
    public static Set<Location> parse(String locations) {
        return Arrays.stream(locations.split(","))
            .map(ss -> ss.split(":"))
            .map(ss -> Location.builder().host(ss[0]).port(Integer.parseInt(ss[1])).build())
            .collect(Collectors.toSet());
    }

    @SneakyThrows
    public static CoordinatorService coordinatorService(Set<Location> locations) {
        Parameters.notEmpty(locations, "locations");
        return new ServiceCaller<>(
            coordinatorCache.get(locations), DEFAULT_RETRY_TIMES, DEFAULT, CoordinatorService.Impl::new
        ).getService();
    }

    @SneakyThrows
    public static MetaService metaService(Set<Location> locations) {
        Parameters.notEmpty(locations, "locations");
        return new ServiceCaller<>(metaCache.get(locations), DEFAULT_RETRY_TIMES, DEFAULT, MetaService.Impl::new
        ).getService();
    }

    @SneakyThrows
    public static MetaService tsoService(Set<Location> locations) {
        Parameters.notEmpty(locations, "locations");
        return new ServiceCaller<>(
            tsoCache.get(locations), DEFAULT_RETRY_TIMES, DEFAULT, MetaService.Impl::new
        ).getService();
    }

    @SneakyThrows
    public static MetaService autoIncrementMetaService(Set<Location> locations) {
        Parameters.notEmpty(locations, "locations");
        return new ServiceCaller<>(
            autoIncrementCache.get(locations), DEFAULT_RETRY_TIMES, DEFAULT, MetaService.Impl::new
        ).getService();
    }

    @SneakyThrows
    public static VersionService versionService(Set<Location> locations) {
        Parameters.notEmpty(locations, "locations");
        return new ServiceCaller<>(
            versionCache.get(locations), DEFAULT_RETRY_TIMES, DEFAULT, VersionService.Impl::new
        ).getService();
    }

    @SneakyThrows
    public static ChannelProvider regionChannelProvider(
        Set<Location> locations, DingoCommonId tableId, DingoCommonId regionId
    ) {
        return tableFailOverCache.get(locations).get(tableId).createRegionProvider(regionId);
    }

    @SneakyThrows
    public static long findRegion(Set<Location> locations, byte[] key) {
        return regionChannelProvider(locations, key).getRegionId();
    }

    @SneakyThrows
    public static long findRegionNewly(Set<Location> locations, byte[] key) {
        return regionChannelProviderNewly(locations, key).getRegionId();
    }

    @SneakyThrows
    public static RegionChannelProvider regionChannelProvider(
        Set<Location> locations, byte[] key
    ) {
        NavigableMap<byte[], ScanRegionInfo> regions = Optional.ofNullable(rangeRegions.floorEntry(key))
            .map(Map.Entry::getValue)
            .ifAbsentSet(() -> {
                long id = CodecUtils.readId(key);
                byte[] begin = CodecUtils.encodeId(key[0], id);
                return rangeRegions.computeIfAbsent(
                    begin, k -> new TreeMap<>(ByteArrayUtils::compare)
                );
            })
            .get();
        synchronized (regions) {
            ScanRegionInfo region = Optional.mapOrNull(regions.floorEntry(key), Map.Entry::getValue);
            if (region != null) {
                Range range = Optional.mapOrNull(region, ScanRegionInfo::getRange);
                if (range != null && compare(key, range.getStartKey()) >= 0 && compare(key, range.getEndKey()) < 0) {
                    RegionChannelProvider regionProvider = regionCache.get(locations).get(region.getRegionId());
                    if (regionProvider.isIn(key)) {
                        return regionProvider;
                    }
                }
            }
            return regionChannelProviderNewly(locations, key);
        }
    }

    @SneakyThrows
    public static RegionChannelProvider regionChannelProviderNewly(
        Set<Location> locations, byte[] key
    ) {
        long id = CodecUtils.readId(key);
        byte[] begin = CodecUtils.encodeId(key[0], id);
        byte[] end = CodecUtils.encodeId(key[0], id + 1);
        NavigableMap<byte[], ScanRegionInfo> regions = rangeRegions.computeIfAbsent(
            begin, k -> new TreeMap<>(ByteArrayUtils::compare)
        );
        synchronized (regions) {
            regions.clear();
            Optional.ofNullable(coordinatorService(locations).scanRegions(
                ScanRegionsRequest.builder().key(begin).rangeEnd(end).build()
            )).map(ScanRegionsResponse::getRegions)
                .ifPresent($ -> $.forEach(region -> regions.put(region.getRange().getStartKey(), region)));
            ScanRegionInfo region = Optional.mapOrNull(regions.floorEntry(key), Map.Entry::getValue);
            Range range = Optional.mapOrNull(region, ScanRegionInfo::getRange);
            if (range != null && compare(key, range.getStartKey()) >= 0 && compare(key, range.getEndKey()) < 0) {
                RegionChannelProvider regionProvider = regionCache.get(locations).get(region.getRegionId());
                if (regionProvider.isIn(key)) {
                    return regionProvider;
                }
            }
            throw new RuntimeException("Cannot found region for " + Arrays.toString(key));
        }
    }

    @SneakyThrows
    public static StoreService storeRegionService(
        Set<Location> locations, byte[] key, int retry
    ) {
        return new ServiceCaller<>(
            regionChannelProvider(locations, key), retry, DEFAULT, StoreService.Impl::new
        ).getService();
    }

    @SneakyThrows
    public static IndexService indexRegionService(
        Set<Location> locations, byte[] key, int retry
    ) {
        return new ServiceCaller<>(
            regionChannelProvider(locations, key), retry, DEFAULT, IndexService.Impl::new
        ).getService();
    }

    @SneakyThrows
    public static StoreService storeRegionService(
        Set<Location> locations, long regionId, int retry
    ) {
        Set<Location> locationsStr = Parameters.notEmpty(locations, "locations");
        ChannelProvider regionProvider = regionCache.get(locationsStr).get(regionId);
        return new ServiceCaller<>(regionProvider, retry, DEFAULT, StoreService.Impl::new).getService();
    }

    @SneakyThrows
    public static IndexService indexRegionService(
        Set<Location> locations, long regionId, int retry
    ) {
        Set<Location> locationsStr = Parameters.notEmpty(locations, "locations");
        ChannelProvider regionProvider = regionCache.get(locationsStr).get(regionId);
        return new ServiceCaller<>(regionProvider, retry, DEFAULT, IndexService.Impl::new).getService();
    }

    @SneakyThrows
    public static StoreService storeRegionService(
        Set<Location> locations, DingoCommonId tableId, DingoCommonId regionId, int retry
    ) {
        Set<Location> locationsStr = Parameters.notEmpty(locations, "locations");
        ChannelProvider regionProvider = tableFailOverCache.get(locationsStr).get(tableId)
            .createRegionProvider(regionId);
        return new ServiceCaller<>(regionProvider, retry, DEFAULT, StoreService.Impl::new).getService();
    }

    @SneakyThrows
    public static IndexService indexRegionService(
        Set<Location> locations, DingoCommonId indexId, DingoCommonId regionId, int retry
    ) {
        Set<Location> locationsStr = Parameters.notEmpty(locations, "locations");
        ChannelProvider regionProvider = tableFailOverCache.get(locationsStr).get(indexId)
            .createRegionProvider(regionId);
        return new ServiceCaller<>(regionProvider, retry, DEFAULT, IndexService.Impl::new).getService();
    }

}
