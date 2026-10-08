-- Besøgendes-nøgle: en tilfældig identitet, browseren gemmer lokalt og sender
-- med ved tilmelding. Den løser to ting på én gang.
--
-- 1) Værn mod lod-farming. Med n lodder er den forventede bedste placering
--    percentil 1/(n+1) — ti faner giver altså forventet en plads blandt de
--    øverste 9 % af feltet. Under ren FIFO giver ti faner næsten ingenting,
--    fordi de rammer samme millisekund. Lodtrækningen FORSTÆRKER farming, så
--    værnet er en forudsætning for at kunne kalde featuren retfærdig — ikke en
--    senere forbedring.
--
-- 2) Genoptagelse. Åbner brugeren fanen igen efter at have været i en anden
--    app, kalder scriptet tilmeld med samme nøgle og får sin eksisterende
--    plads tilbage — samme nummer, samme lod.
--
-- Reglen SKAL håndhæves af databasen, ikke kun af koden. Servicelaget gør
-- "find levende, ellers opret", og mellem opslaget og indsættelsen er der et
-- vindue: to samtidige faner kan begge finde ingenting og begge oprette. Så
-- har den ene besøgende to lodder. Samme argument som for ux_queue_active_-
-- external_reference i V9 — databasen skal holde reglen, ikke rækkefølgen.
ALTER TABLE queue_session
    ADD COLUMN visitor_key VARCHAR(64) NULL;

-- Unikheden gælder kun LEVENDE sessioner: samme browser skal gerne kunne
-- stille sig i kø igen, efter den forrige session er lukket. Derfor det samme
-- greb som i V9 — en genereret kolonne, der kun har værdi mens rækken er
-- levende, og et unikt indeks ovenpå. MySQL regner NULL-værdier som indbyrdes
-- forskellige i et unikt indeks, så alle lukkede sessioner falder udenfor.
--
-- Hvorfor closed_at og ikke state:
--   * state <> 'CLOSED' ville binde et enum-strengliteral ind i skemaet. Enum-
--     kolonner er netop forankret som VARCHAR, for at nye enum-værdier ikke
--     kræver ALTER TABLE; et sådant udtryk ville genindføre koblingen.
--   * closed_at sættes én gang og ændres aldrig, så den genererede kolonne
--     skifter præcis én gang (værdi -> NULL). state ændres tre gange pr.
--     session på systemets varmeste sti, uden at resultatet ændrer sig de to
--     første gange.
--   * closed_at IS NULL har samme form som archived_at IS NULL i V9.
-- De to udtryk er semantisk ens: closeInternal sætter closed_at, close_reason
-- og state i samme transaktion, så de kan ikke komme ud af trit.
ALTER TABLE queue_session
    ADD COLUMN active_visitor_key VARCHAR(64)
        GENERATED ALWAYS AS (CASE WHEN closed_at IS NULL THEN visitor_key END) STORED;

-- Sammensat, fordi den samme besøgende gerne må stå i kø til flere events
-- samtidig — unikheden er pr. (kø, besøgende), ikke global.
--
-- Indekset bærer samtidig tilmeldingens opslag ("findes der en levende session
-- for denne besøgende i denne kø?"), så et separat indeks på visitor_key ville
-- kun være endnu et indeks at vedligeholde på ankomststien.
CREATE UNIQUE INDEX ux_session_active_visitor ON queue_session (queue_id, active_visitor_key);
