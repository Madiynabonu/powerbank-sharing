package com.powerbank.stationservice.grpc;

import com.powerbank.station.grpc.GetStationRequest;
import com.powerbank.station.grpc.GetStationsRequest;
import com.powerbank.station.grpc.GetStationsResponse;
import com.powerbank.station.grpc.Station;
import com.powerbank.station.grpc.StationApiGrpc;
import com.powerbank.stationservice.service.StationService;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * gRPC server implementation that delegates to {@link StationService}.
 * Kong's gRPC-gateway plugin transcodes incoming REST calls to this service
 * based on the (google.api.http) annotations in station.proto.
 */
@GrpcService
@Slf4j
@RequiredArgsConstructor
public class StationGrpcService extends StationApiGrpc.StationApiImplBase {

    private final StationService stationService;

    @Override
    public void getStations(GetStationsRequest request,
                            StreamObserver<GetStationsResponse> responseObserver) {
        log.debug("gRPC getStations lat={} lng={} radius={} limit={}",
                request.getLat(), request.getLng(), request.getRadiusM(), request.getLimit());
        try {
            List<com.powerbank.stationservice.domain.Station> stations =
                    stationService.findNearby(
                            request.getLat(), request.getLng(),
                            request.getRadiusM(), request.getLimit());

            GetStationsResponse.Builder response = GetStationsResponse.newBuilder();
            stations.forEach(s -> response.addStations(toProto(s, request.getLat(), request.getLng())));
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("getStations failed", e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void getStation(GetStationRequest request,
                           StreamObserver<Station> responseObserver) {
        log.debug("gRPC getStation id={}", request.getId());
        stationService.findById(request.getId())
                .map(s -> toProto(s, 0, 0))
                .ifPresentOrElse(
                        proto -> {
                            responseObserver.onNext(proto);
                            responseObserver.onCompleted();
                        },
                        () -> responseObserver.onError(
                                Status.NOT_FOUND
                                        .withDescription("Station not found: " + request.getId())
                                        .asRuntimeException()));
    }

    private Station toProto(com.powerbank.stationservice.domain.Station s,
                             double queryLat, double queryLng) {
        Station.Builder builder = Station.newBuilder()
                .setId(s.getId().toString())
                .setName(s.getName())
                .setLat(s.getLat())
                .setLng(s.getLng())
                .setStatus(s.getStatus().name())
                .setTotalSlots(s.getTotalSlots())
                .setAvailablePowerbanks((int) s.availablePowerBanks())
                .setFreeSlots((int) s.freeSlots());

        if (queryLat != 0 || queryLng != 0) {
            builder.setDistanceM(haversineMetres(queryLat, queryLng, s.getLat(), s.getLng()));
        }
        return builder.build();
    }

    private static double haversineMetres(double lat1, double lng1, double lat2, double lng2) {
        final double R = 6_371_000;
        double dLat = Math.toRadians(lat2 - lat1);
        double dLng = Math.toRadians(lng2 - lng1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLng / 2) * Math.sin(dLng / 2);
        return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }
}
