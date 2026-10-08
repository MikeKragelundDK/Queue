-- Abonnementsknuden får en TYPE i stedet for et forretningsnavn.
--
-- Kø-træet skal ikke afspejle platformens prismodel, kun hvordan kapacitet
-- deles. To regler genkendte hidtil abonnementet på navnet ("basis") og var
-- dermed ét omdøb fra tavst at holde op med at virke.
--
-- Det er ikke to nye QueueLevel-værdier: niveauet koder træets form, og de to
-- typer har samme dybde, forælder og lovlige børn. Som niveauer ville
-- form-invarianten få to næsten identiske grene at vedligeholde hvert eneste
-- sted, den håndhæves.
--
-- Typen er null på alt andet end SUBSCRIPTION-knuder.
ALTER TABLE queue
    ADD COLUMN queue_type VARCHAR(16) NULL;

-- Der følger bevidst INGEN data-migration med — hverken typer på de gamle
-- abonnementer eller omdøb af dem.
--
-- En data-migration bærer data videre, som ikke kan bygges igen. Sådan data
-- findes ikke: dev- og testbaserne fødes tomme ved hver opstart og bygges af
-- DevDataSeeder, og der findes endnu intet udrullet miljø. Hver eneste UPDATE
-- ville ramme nul rækker.
--
-- Værre endnu ville den beskrive træets form et andet sted end seederen, og så
-- ville de to kunne komme til at sige hver sit — en frisk base ville få
-- seederens svar og en migreret base migrationens. Formen står ét sted, og det
-- er seederen.
--
-- Den dag der findes en base, som ikke kan smides væk, vender argumentet: en
-- anvendt migration kan ikke fjernes igen, for Flyway validerer historikken mod
-- filerne. Fra da af skal ændringer af eksisterende rækker ske i en migration.

-- Præcis én aktiv knude af hver type — håndhævet af databasen og ikke af
-- servicelaget: to pods kunne begge finde ingen priority og begge oprette en.
--
-- Samme greb som ux_queue_active_external_reference (V9) og
-- ux_session_active_visitor (V12): en genereret kolonne med værdi kun mens
-- rækken er aktiv. Udtrykket nævner bevidst kun archived_at og ikke level —
-- enum-kolonner er forankret som VARCHAR, netop for at nye værdier ikke kræver
-- ALTER TABLE, og et niveau-literal her ville genindføre koblingen. Typen er
-- alligevel null uden for abonnementslaget, og MySQL regner NULL-værdier som
-- indbyrdes forskellige i et unikt indeks, så resten af træet falder udenfor af
-- sig selv.
ALTER TABLE queue
    ADD COLUMN active_queue_type VARCHAR(16)
        GENERATED ALWAYS AS (CASE WHEN archived_at IS NULL THEN queue_type END) STORED;

CREATE UNIQUE INDEX ux_queue_active_type ON queue (active_queue_type);
