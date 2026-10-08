-- 1) Aktiveringsmotorens kandidat-forespørgsel sorterer globalt på
--    sequence_number på tværs af mange køer. ix_session_queue_state_seq starter
--    med queue_id og kan derfor ikke bære den sortering — uden dette indeks
--    filesorterer MySQL hele backloggen for hver side i hvert tick.
CREATE INDEX ix_session_state_seq ON queue_session (state, sequence_number);

-- 2) Motoren finder de dirty køer hvert tick; uden indeks er det en fuld
--    tabelscanning af queue hver gang.
CREATE INDEX ix_queue_dirty ON queue (dirty);

-- 3) Idempotens-værn for indgående platform-events: der må kun findes én
--    AKTIV kø pr. externalReference. Handleren gør "find aktiv, ellers opret",
--    og to pods, der får samme gensendte event, kan begge finde ingenting og
--    begge oprette. Databasen skal holde reglen, ikke kun rækkefølgen.
--
--    Arkivering er soft delete, så arkiverede knuder beholder deres nøgle og må
--    gerne optræde flere gange (samme arrangør kan være oprettet og arkiveret
--    igen og igen). Derfor et unique-indeks på en genereret kolonne, der kun
--    har en værdi, mens knuden er aktiv: MySQL regner NULL-værdier som
--    indbyrdes forskellige i et unique-indeks, så alle arkiverede rækker — og
--    alle knuder uden ekstern nøgle (GLOBAL, SUBSCRIPTION) — falder udenfor.
--    Værn før indekset oprettes: intet håndhævede reglen tidligere, så en
--    langlivet database kan allerede have to AKTIVE køer på samme nøgle. Uden
--    denne oprydning fejler CREATE UNIQUE INDEX, og da Flyway kører ved
--    opstart med Hibernate på validate, ville podden slet ikke starte.
--    Nyeste aktive knude vinder; de ældre arkiveres (soft delete, så intet
--    datagrundlag går tabt) — samme semantik som resten af systemet.
UPDATE queue q
    JOIN (
        SELECT external_reference, MAX(id) AS keep_id
        FROM queue
        WHERE archived_at IS NULL AND external_reference IS NOT NULL
        GROUP BY external_reference
        HAVING COUNT(*) > 1
    ) dubletter ON q.external_reference = dubletter.external_reference
SET q.archived_at = CURRENT_TIMESTAMP(6)
WHERE q.archived_at IS NULL AND q.id <> dubletter.keep_id;

ALTER TABLE queue
    ADD COLUMN active_external_reference VARCHAR(255)
        GENERATED ALWAYS AS (CASE WHEN archived_at IS NULL THEN external_reference END) STORED;

CREATE UNIQUE INDEX ux_queue_active_external_reference ON queue (active_external_reference);

-- 4) Opslag på ekstern nøgle sker ved hvert eneste platform-event; uden indeks
--    er det en fuld tabelscanning pr. besked.
CREATE INDEX ix_queue_external_reference ON queue (external_reference);
