package com.powerbank.rentalservice.grpc;

import com.powerbank.rental.grpc.CreateRentalRequest;
import com.powerbank.rental.grpc.CreateRentalResponse;
import com.powerbank.rental.grpc.FinishRentalRequest;
import com.powerbank.rental.grpc.GetRentalHistoryRequest;
import com.powerbank.rental.grpc.GetRentalHistoryResponse;
import com.powerbank.rental.grpc.GetRentalStatusRequest;
import com.powerbank.rental.grpc.GetStationRequest;
import com.powerbank.rental.grpc.GetStationsRequest;
import com.powerbank.rental.grpc.GetStationsResponse;
import com.powerbank.rental.grpc.RentalApiGrpc;
import com.powerbank.rental.grpc.RentalStatusResponse;
import com.powerbank.rental.grpc.RentalSummary;
import com.powerbank.rental.grpc.StationDto;
import com.powerbank.rentalservice.domain.Rental;
import com.powerbank.rentalservice.service.RentalService;
import com.powerbank.station.grpc.StationApiGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.data.domain.Page;

@GrpcService
@Slf4j
@RequiredArgsConstructor
public class RentalGrpcService extends RentalApiGrpc.RentalApiImplBase {

    private final RentalService rentalService;

    @GrpcClient("station-service")
    private StationApiGrpc.StationApiBlockingStub stationStub;

    @Override
    public void createRental(CreateRentalRequest request,
                             StreamObserver<CreateRentalResponse> responseObserver) {
        try {
            UUID userId = UUID.fromString(request.getUserId());
            UUID stationId = UUID.fromString(request.getStationId());
            UUID cardId = UUID.fromString(request.getCardId());
            Rental rental = rentalService.create(userId, stationId, cardId);
            responseObserver.onNext(CreateRentalResponse.newBuilder()
                    .setRentalId(rental.getId().toString())
                    .setStatus(rental.getStatus().name())
                    .build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("createRental failed", e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void getRentalStatus(GetRentalStatusRequest request,
                                StreamObserver<RentalStatusResponse> responseObserver) {
        try {
            rentalService.findById(UUID.fromString(request.getId()))
                    .map(this::toStatusProto)
                    .ifPresentOrElse(
                            proto -> { responseObserver.onNext(proto); responseObserver.onCompleted(); },
                            () -> responseObserver.onError(
                                    Status.NOT_FOUND.withDescription("Rental not found: " + request.getId())
                                            .asRuntimeException()));
        } catch (IllegalArgumentException e) {
            responseObserver.onError(
                    Status.INVALID_ARGUMENT.withDescription("Invalid rental ID: " + request.getId())
                            .asRuntimeException());
        }
    }

    @Override
    public void getRentalHistory(GetRentalHistoryRequest request,
                                 StreamObserver<GetRentalHistoryResponse> responseObserver) {
        UUID userId;
        try {
            userId = UUID.fromString(request.getUserId());
        } catch (IllegalArgumentException e) {
            responseObserver.onError(
                    Status.INVALID_ARGUMENT.withDescription("Invalid user ID: " + request.getUserId())
                            .asRuntimeException());
            return;
        }
        try {
            Page<Rental> page = rentalService.findHistory(userId, request.getPage(), request.getPageSize());
            GetRentalHistoryResponse.Builder resp = GetRentalHistoryResponse.newBuilder()
                    .setTotal((int) page.getTotalElements());
            page.forEach(r -> resp.addRentals(toSummaryProto(r)));
            responseObserver.onNext(resp.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("getRentalHistory failed", e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void finishRental(FinishRentalRequest request,
                             StreamObserver<RentalStatusResponse> responseObserver) {
        UUID rentalId, returnStationId;
        try {
            rentalId = UUID.fromString(request.getRentalId());
            returnStationId = UUID.fromString(request.getStationId());
        } catch (IllegalArgumentException e) {
            responseObserver.onError(
                    Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
            return;
        }
        try {
            Rental rental = rentalService.finish(rentalId, returnStationId);
            responseObserver.onNext(toStatusProto(rental));
            responseObserver.onCompleted();
        } catch (IllegalArgumentException e) {
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asRuntimeException());
        } catch (IllegalStateException e) {
            responseObserver.onError(Status.FAILED_PRECONDITION.withDescription(e.getMessage()).asRuntimeException());
        } catch (Exception e) {
            log.error("finishRental failed", e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void getStations(GetStationsRequest request,
                            StreamObserver<GetStationsResponse> responseObserver) {
        try {
            com.powerbank.station.grpc.GetStationsRequest stationReq =
                    com.powerbank.station.grpc.GetStationsRequest.newBuilder()
                            .setLat(request.getLat())
                            .setLng(request.getLng())
                            .setRadiusM(request.getRadiusM())
                            .setLimit(request.getLimit())
                            .build();
            com.powerbank.station.grpc.GetStationsResponse stationResp = stationStub.getStations(stationReq);
            GetStationsResponse.Builder resp = GetStationsResponse.newBuilder();
            stationResp.getStationsList().forEach(s -> resp.addStations(toStationDto(s)));
            responseObserver.onNext(resp.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("getStations failed", e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void getStation(GetStationRequest request,
                           StreamObserver<StationDto> responseObserver) {
        try {
            com.powerbank.station.grpc.GetStationRequest stationReq =
                    com.powerbank.station.grpc.GetStationRequest.newBuilder()
                            .setId(request.getId())
                            .build();
            com.powerbank.station.grpc.Station s = stationStub.getStation(stationReq);
            responseObserver.onNext(toStationDto(s));
            responseObserver.onCompleted();
        } catch (io.grpc.StatusRuntimeException e) {
            responseObserver.onError(e);
        } catch (Exception e) {
            log.error("getStation failed", e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    private StationDto toStationDto(com.powerbank.station.grpc.Station s) {
        return StationDto.newBuilder()
                .setId(s.getId())
                .setName(s.getName())
                .setLat(s.getLat())
                .setLng(s.getLng())
                .setStatus(s.getStatus())
                .setTotalSlots(s.getTotalSlots())
                .setAvailablePowerbanks(s.getAvailablePowerbanks())
                .setFreeSlots(s.getFreeSlots())
                .setDistanceM(s.getDistanceM())
                .build();
    }

    private RentalStatusResponse toStatusProto(Rental r) {
        RentalStatusResponse.Builder b = RentalStatusResponse.newBuilder()
                .setRentalId(r.getId().toString())
                .setStatus(r.getStatus().name());
        if (r.getPowerbankId() != null) b.setPowerbankId(r.getPowerbankId().toString());
        if (r.getSlotNumber() != null) b.setSlot(r.getSlotNumber());
        if (r.getFailureReason() != null) b.setFailureReason(r.getFailureReason());
        return b.build();
    }

    private RentalSummary toSummaryProto(Rental r) {
        RentalSummary.Builder b = RentalSummary.newBuilder()
                .setRentalId(r.getId().toString())
                .setStationId(r.getStationId().toString())
                .setStatus(r.getStatus().name());
        if (r.getPowerbankId() != null) b.setPowerbankId(r.getPowerbankId().toString());
        if (r.getStartedAt() != null) b.setStartedAt(r.getStartedAt().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        if (r.getFinishedAt() != null) b.setFinishedAt(r.getFinishedAt().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        return b.build();
    }
}
