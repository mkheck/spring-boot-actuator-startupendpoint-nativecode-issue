package com.thehecklers.thing2;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.metrics.buffering.BufferingApplicationStartup;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.nativex.hint.TypeHint;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;

import java.time.Instant;

@SpringBootApplication
@TypeHint(types = {Aircraft.class, Weather.class}, typeNames = {"com.thehecklers.thing2.Aircraft", "com.thehecklers.thing2.Weather"})
public class Thing2Application {

    public static void main(String[] args) {
        Hooks.onErrorDropped(e -> System.out.println("Connection closed 👋"));
//        SpringApplication.run(Thing2Application.class, args);
        SpringApplication app = new SpringApplication(Thing2Application.class);
        app.setApplicationStartup(new BufferingApplicationStartup(2048));
        app.run(args);
    }

    @Bean
    RSocketRequester requester(RSocketRequester.Builder builder) {
        return builder.tcp("192.168.1.152", 7635);
    }
}

@Controller
@AllArgsConstructor
class Thing2Controller {
    private final RSocketRequester requester;

    // Request/reponse
    @MessageMapping("reqresp")
    Mono<Aircraft> reqResp(Mono<Instant> timestampMono) {
        return timestampMono.doOnNext(ts -> System.out.println("⏱ " + ts))
                .then(requester.route("acstream")
                        .data(Instant.now())
                        .retrieveFlux(Aircraft.class)
                        .next());
    }

    // Request/stream
    @MessageMapping("reqstream")
    Flux<Aircraft> reqStream(Mono<Instant> timestampMono) {
        return timestampMono.doOnNext(ts -> System.out.println("⏱ " + ts))
                .thenMany(requester.route("acstream")
                        .data(Instant.now())
                        .retrieveFlux(Aircraft.class));
    }

    // Fire and forget
    @MessageMapping("fireforget")
    Mono<Void> fireForget(Mono<Weather> weatherMono) {
        return weatherMono.doOnNext(wx -> System.out.println("🌧 " + wx))
                .then();
    }

    // Bidirectional channel
    @MessageMapping("channel")
    Flux<Aircraft> channel(Flux<Weather> weatherFlux) {
        return weatherFlux.doOnSubscribe(sub -> System.out.println("SUBSCRIBED TO WEATHER FEED!"))
                .doOnNext(wx -> System.out.println("☀️ " + wx))
                .switchMap(wx -> requester.route("acstream")
                        .data(Instant.now())
                        .retrieveFlux(Aircraft.class));
    }
}

@Data
class Weather {
    private Instant when;
    private String observation;
}

@Data
class Aircraft {
    private String callsign, reg, flightno, type;
    private int altitude, heading, speed;
    private double lat, lon;
}