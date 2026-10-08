# Kø-typer: normal og priority

Kø-træet bruger NORMAL og PRIORITY. "Pro plus" svarer til PRIORITY; øvrige
abonnementer til NORMAL. Events og medlemssystemer følger samme regler.

Status: typer, generiske knuder, routing og dræning er implementeret
(KPM-84–88). Periodisk synkronisering og platformens publisher mangler.

## 1. Kø-træ og kapacitet

```text
GLOBAL
├─ NORMAL (SUBSCRIPTION)
│  └─ generisk arrangør (ORGANIZER, maxCapacity = null)
│     ├─ generisk event-kø (EVENT, eget loft)
│     └─ generisk medlems-kø (MEMBERSYSTEM, eget loft)
└─ PRIORITY (SUBSCRIPTION)
   └─ arrangør pr. kunde (ORGANIZER)
      ├─ event pr. event (EVENT)
      └─ medlemssystem (MEMBERSYSTEM)
```

Normale kunder deler tre generiske knuder. De to klienttyper har hvert sit
blad og loft; NORMAL og GLOBAL er fælles overordnede lofter. Sessioner ligger
på blade, og kapacitet kontrolleres langs stien fra rod til blad.

`QueueService` afviser yderligere knuder under NORMAL og flyt ind i eller ud
af fælleskøens gren.

## 2. QueueType

`QueueType` er kun sat på SUBSCRIPTION-knuder. `ux_queue_active_type`
håndhæver højst én aktiv knude af hver type via en genereret kolonne,
der kun har værdi, når `archived_at is null`.

## 3. Routing

En ukendt nøgle routes til klienttypens fælleskø. Normale kunders events
har ingen egne knuder. Tabte oprettelses-events for priority-kunder kan derfor
også udløse fallback; den planlagte synkronisering skal rette manglende knuder.

## 4. Planlagt synkronisering

Platformen skal levere et periodisk snapshot af priority-kunder og deres
blade som supplement til RabbitMQ-events. Funktionen er ikke implementeret.

- Tomme svar og fald over en fastsat tærskel afvises og logges.
- Fjernede kunder sættes til dræning. Genoptræder de, ryddes flaget.
- HTTP-kald kræver autentifikation (KPM-54), timeout og retry. Ved fejl
  fortsætter køen med sit eksisterende træ.
- Jobbet får sin egen lås-række; næste ledige id er 6.

Ved vellykkede kørsler begrænser intervallet tiden med manglende knuder.

## 5. Typeskift

PRIORITY → NORMAL: `drainingAt` sættes på grenen. Nye tilmeldinger går til
fælleskøen; eksisterende sessioner behandles som før og tæller fortsat under
PRIORITY. `QueueDrainingJob` (lås-række 5) arkiverer grenen, når den er tom.
En opgradering under dræningen rydder flaget.

NORMAL → PRIORITY: platformens publisher skal sende `organizer.created`
efterfulgt af `event.created` og `membersystem.created` for kundens blade.
Handlerne er idempotente. Publisheren er endnu ikke implementeret.

Sessioner i fælleskøen bliver der ved opgradering. Flytning mellem grene er
fravalgt, fordi det ville ændre modtagergrenens FIFO-rækkefølge.

## 6. Statistik og begrænsninger

Statistik pr. arrangør og event findes kun for PRIORITY. NORMAL viser samlede
tal for de to generiske blade og kan ikke have event-specifikke lofter.

## 7. Uafklaret

- Synkroniseringsinterval og tærskel for afvisning af ufuldstændige snapshots.
- Navne på de generiske knuder i GUI'et.
