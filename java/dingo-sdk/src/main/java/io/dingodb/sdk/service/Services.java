package io.dingodb.sdk.service;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.dingodb.sdk.common.utils.Parameters;
import io.dingodb.sdk.service.caller.ServiceCaller;
import io.dingodb.sdk.service.entity.common.Location;
import io.dingodb.sdk.service.entity.coordinator.GetCoordinatorMapResponse;
import io.dingodb.sdk.service.entity.meta.DingoCommonId;
import lombok.SneakyThrows;

import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.grpc.CallOptions.DEFAULT;

public class Services {

    public static final int DEFAULT_RETRY_TIMES = 30;

    private static final LoadingCache<Set<Location>, CoordinatorService> coordinatorServiceCache = CacheBuilder
        .newBuilder()
        .expireAfterAccess(60, TimeUnit.MINUTES)
        .expireAfterWrite(60, TimeUnit.MINUTES)
        .maximumSize(8)
        .build(new CacheLoader<Set<Location>, CoordinatorService>() {
            @Override
            public CoordinatorService load(Set<Location> key) {
                return (CoordinatorService) Proxy.newProxyInstance(
                    CoordinatorService.class.getClassLoader(),
                    new Class[] {CoordinatorService.class},
                    new ServiceCaller<CoordinatorService>(
                        coordinatorServiceChannelProvider(key), DEFAULT_RETRY_TIMES, DEFAULT
                    ){}
                );
            }
        });

    private static final LoadingCache<Set<Location>, MetaService> autoIncrementServiceCache = CacheBuilder
        .newBuilder()
        .expireAfterAccess(60, TimeUnit.MINUTES)
        .expireAfterWrite(60, TimeUnit.MINUTES)
        .maximumSize(8)
        .build(new CacheLoader<Set<Location>, MetaService>() {
            @Override
            public MetaService load(Set<Location> key) {
                return (MetaService) Proxy.newProxyInstance(
                    MetaService.class.getClassLoader(),
                    new Class[] {MetaService.class},
                    new ServiceCaller<MetaService>(
                        autoIncrementChannelProvider(key), DEFAULT_RETRY_TIMES, DEFAULT
                    ){}
                );
            }
        });

    private static final LoadingCache<Set<Location>, MetaService> metaServiceCache = CacheBuilder
        .newBuilder()
        .expireAfterAccess(60, TimeUnit.MINUTES)
        .expireAfterWrite(60, TimeUnit.MINUTES)
        .maximumSize(8)
        .build(new CacheLoader<Set<Location>, MetaService>() {
            @Override
            public MetaService load(Set<Location> key) {
                return (MetaService) Proxy.newProxyInstance(
                    MetaService.class.getClassLoader(),
                    new Class[] {MetaService.class},
                    new ServiceCaller<MetaService>(
                        metaServiceChannelProvider(key), DEFAULT_RETRY_TIMES, DEFAULT
                    ){}
                );
            }
        });

    private static final LoadingCache<Set<Location>, VersionService> versionServiceCache = CacheBuilder
        .newBuilder()
        .expireAfterAccess(60, TimeUnit.MINUTES)
        .expireAfterWrite(60, TimeUnit.MINUTES)
        .maximumSize(8)
        .build(new CacheLoader<Set<Location>, VersionService>() {
            @Override
            public VersionService load(Set<Location> key) {
                return (VersionService) Proxy.newProxyInstance(
                    VersionService.class.getClassLoader(),
                    new Class[] {VersionService.class},
                    new ServiceCaller<VersionService>(
                        kvServiceChannelProvider(key), DEFAULT_RETRY_TIMES, DEFAULT
                    ){}
                );
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
                        .build(new CacheLoader<DingoCommonId, TableRegionsFailOver>() {
                            @Override
                            public TableRegionsFailOver load(DingoCommonId tableId) throws Exception {
                                return new TableRegionsFailOver(tableId, metaServiceCache.get(locations));
                            }
                        });
                }
            });

    public static CoordinatorChannelProvider coordinatorServiceChannelProvider(Set<Location> locations) {
        return new CoordinatorChannelProvider(locations, GetCoordinatorMapResponse::getLeaderLocation);
    }

    public static CoordinatorChannelProvider metaServiceChannelProvider(Set<Location> locations) {
        return new CoordinatorChannelProvider(locations, GetCoordinatorMapResponse::getLeaderLocation);
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
        return coordinatorServiceCache.get(Parameters.notEmpty(locations, "locations"));
    }

    @SneakyThrows
    public static MetaService metaService(Set<Location> locations) {
        return metaServiceCache.get(Parameters.notEmpty(locations, "locations"));
    }

    @SneakyThrows
    public static MetaService autoIncrementMetaService(Set<Location> locations) {
        return autoIncrementServiceCache.get(Parameters.notEmpty(locations, "locations"));
    }

    @SneakyThrows
    public static VersionService versionService(Set<Location> locations) {
        return versionServiceCache.get(Parameters.notEmpty(locations, "locations"));
    }

    @SneakyThrows
    public static StoreService regionService(
        Set<Location> locations, DingoCommonId tableId, DingoCommonId regionId, int retry
    ) {
        Set<Location> locationsStr = Parameters.notEmpty(locations, "locations");
        ChannelProvider regionProvider = tableFailOverCache.get(locationsStr).get(tableId)
            .createRegionProvider(regionId);
        ServiceCaller<StoreService> serviceCaller = new ServiceCaller<StoreService>(
            regionProvider, retry, DEFAULT
        ){};
        serviceCaller.addExcludeErrorCheck(StoreService.kvScanContinue);
        serviceCaller.addExcludeErrorCheck(StoreService.kvScanRelease);
        return (StoreService) Proxy.newProxyInstance(
            StoreService.class.getClassLoader(),
            new Class[] {StoreService.class}, serviceCaller
        );
    }

}
