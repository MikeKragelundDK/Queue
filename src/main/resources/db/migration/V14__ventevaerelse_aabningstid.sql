-- Venteværelset får sit eget åbningstidspunkt ved siden af salgsstarten.
--
-- Salgsstart hed opens_at, men med to tidspunkter ville det navn betyde det
-- modsatte af, hvad feltet gør: opens_at læses som "køen åbner", og det er
-- netop det NYE felt. Salgsstart hedder derfor sales_start_at, og opens_at
-- overtages af åbningstidspunktet.
--
-- drawAt blev fravalgt som navn: det ville stå ét bogstav fra drawn_at
-- (hvornår lodtrækningen faktisk kørte) i samme klasse.
ALTER TABLE queue
    CHANGE COLUMN opens_at sales_start_at DATETIME(6) NULL,
    ADD COLUMN opens_at DATETIME(6) NULL;
