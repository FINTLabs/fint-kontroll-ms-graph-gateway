FROM gradle:9.3.1-jdk25 as builder
USER root
COPY . .
RUN gradle --no-daemon build

FROM gcr.io/distroless/java25
ENV JAVA_TOOL_OPTIONS -XX:+ExitOnOutOfMemoryError
COPY --from=builder /home/gradle/build/libs/fint-kontroll-ms-graph-gateway-*.jar /data/app.jar
CMD ["/data/app.jar"]
