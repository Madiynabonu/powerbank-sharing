package com.powerbank.rentalservice.grpc;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import java.util.Locale;
import net.devh.boot.grpc.server.interceptor.GrpcGlobalServerInterceptor;

@GrpcGlobalServerInterceptor
public class LocaleGrpcInterceptor implements ServerInterceptor {

    public static final Context.Key<Locale> LOCALE_KEY = Context.key("locale");

    private static final Metadata.Key<String> ACCEPT_LANGUAGE =
            Metadata.Key.of("accept-language", Metadata.ASCII_STRING_MARSHALLER);

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        Locale locale = parseLocale(headers.get(ACCEPT_LANGUAGE));
        Context ctx = Context.current().withValue(LOCALE_KEY, locale);
        return Contexts.interceptCall(ctx, call, headers, next);
    }

    private static Locale parseLocale(String acceptLanguage) {
        if (acceptLanguage == null || acceptLanguage.isBlank()) {
            return Locale.ENGLISH;
        }
        String tag = acceptLanguage.split("[,;]")[0].trim();
        Locale locale = Locale.forLanguageTag(tag);
        return locale.getLanguage().isEmpty() ? Locale.ENGLISH : locale;
    }
}