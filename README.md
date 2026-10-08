# queue-system-v2

Waiting-room/kø-system, der afløser Event It's nuværende kø. Køen beskytter event-booking og medlemsoprettelse på main-platformen mod overbelastning.

Quarkus 3.33 · Java 25 · MySQL · RabbitMQ · Svelte-admin-GUI.

```shell
export JAVA_HOME=$HOME/.jdks/jdk-25.0.4+7
./mvnw quarkus:dev          # dev-mode; Dev Services kræver Docker
./mvnw test                 # tests
./mvnw -DskipTests package  # build
```
