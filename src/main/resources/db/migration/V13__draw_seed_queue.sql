-- Lodtrækningens tilfældighedskilde: 32 bytes fra SecureRandom, gemt som hex.
-- Claimes én gang pr. kø med "update ... where draw_seed is null", så alle pods
-- bruger samme værdi.
--
-- Uden den ville rækkefølgen blive trukket i selve øjeblikket, og to pods, der
-- skriver batches i samme kø, ville flette to forskellige permutationer sammen
--
-- Ryddes bevidst ikke efter lodtrækningen.
ALTER TABLE queue
    ADD draw_seed VARCHAR(64) NULL;
