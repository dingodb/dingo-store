package io.dingodb.sdk.service;

import io.dingodb.sdk.service.caller.ServiceCaller;
import io.dingodb.sdk.service.entity.common.Location;
import io.dingodb.sdk.service.entity.coordinator.GetCoordinatorMapResponse;
import io.dingodb.sdk.service.entity.meta.DingoCommonId;

import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static io.grpc.CallOptions.DEFAULT;

public class Services {

    public static final int DEFAULT_RETRY_TIMES = 30;

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

    public static CoordinatorService coordinatorService(Set<Location> locations, int retry) {
        return (CoordinatorService) Proxy.newProxyInstance(
            CoordinatorService.class.getClassLoader(), 
            new Class[] {CoordinatorService.class},
            new ServiceCaller<CoordinatorService>(
                coordinatorServiceChannelProvider(locations), retry, DEFAULT
            ){}
        );
    }

    public static MetaService metaService(Set<Location> locations, int retry) {
        return (MetaService) Proxy.newProxyInstance(
            MetaService.class.getClassLoader(),
            new Class[] {MetaService.class},
            new ServiceCaller<MetaService>(
                metaServiceChannelProvider(locations), retry, DEFAULT
            ){}
        );
    }

    public static MetaService autoIncrementMetaService(Set<Location> locations, int retry) {
        return (MetaService) Proxy.newProxyInstance(
            MetaService.class.getClassLoader(),
            new Class[] {MetaService.class},
            new ServiceCaller<MetaService>(
                autoIncrementChannelProvider(locations), retry, DEFAULT
            ){}
        );
    }

    public static VersionService versionService(Set<Location> locations, int retry) {
        return (VersionService) Proxy.newProxyInstance(
            VersionService.class.getClassLoader(),
            new Class[] {VersionService.class},
            new ServiceCaller<VersionService>(
                kvServiceChannelProvider(locations), retry, DEFAULT
            ){}
        );
    }

    public static StoreService regionService(
        DingoCommonId tableId, DingoCommonId regionId, int retry,MetaService metaService
    ) {
        ServiceCaller<StoreService> serviceCaller = new ServiceCaller<StoreService>(
            new TableRegionsFailOver(tableId, metaService).createRegionProvider(regionId), retry, DEFAULT) {
        };
        serviceCaller.addExcludeErrorCheck(StoreService.kvScanContinue);
        serviceCaller.addExcludeErrorCheck(StoreService.kvScanRelease);
        return (StoreService) Proxy.newProxyInstance(
            StoreService.class.getClassLoader(),
            new Class[] {StoreService.class}, serviceCaller
        );
    }

}
