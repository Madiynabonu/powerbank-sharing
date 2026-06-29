package com.powerbank.rentalservice.grpc;

import com.powerbank.rental.grpc.CreateRentalRequest;
import com.powerbank.rental.grpc.CreateRentalResponse;
import com.powerbank.rental.grpc.FinishRentalRequest;
import com.powerbank.rental.grpc.GetRentalHistoryRequest;
import com.powerbank.rental.grpc.GetRentalHistoryResponse;
import com.powerbank.rental.grpc.GetRentalStatusRequest;
import com.powerbank.rental.grpc.RentalApiGrpc;
import com.powerbank.rental.grpc.RentalStatusResponse;
import com.powerbank.rental.grpc.RentalSummary;
import com.powerbank.rentalservice.domain.Rental;
import com.powerbank.rentalservice.service.RentalService;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.data.domain.Page;

@GrpcService
@Slf4j
@RequiredArgsConstructor
public class RentalGrpcService extends RentalApiGrpc.RentalApiImplBase {

    private final RentalService rentalService;

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
        rentalService.findById(UUID.fromString(request.getId()))
                .map(this::toStatusProto)
                .ifPresentOrElse(
                        proto -> { responseObserver.onNext(proto); responseObserver.onCompleted(); },
                        () -> responseObserver.onError(
                                Status.NOT_FOUND.withDescription("Rental not found: " + request.getId())
                                        .asRuntimeException()));
    }

    @Override
    public void getRentalHistory(GetRentalHistoryRequest request,
                                 StreamObserver<GetRentalHistoryResponse> responseObserver) {
        try {
            UUID userId = UUID.fromString(request.getUserId());
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
        try {
            UUID rentalId = UUID.fromString(request.getRentalId());
            UUID returnStationId = UUID.fromString(request.getStationId());
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
