-- Dræning: en nedgraderet kundes gren tages ud af drift uden at blive revet væk.
--
-- Skifter en kunde fra priority til normal, må grenen ikke arkiveres med det
-- samme. Arkivering lukker åbne sessioner, og det ville smide folk ud midt i en
-- booking — og folk, der har stået i kø, ud uden at have fået noget for det.
--
-- Grenen sættes derfor til at dræne: nye ankomster for kundens nøgler routes
-- til de generiske køer, mens de sessioner, der allerede står i grenen, lukkes
-- ind og gennemfører som hidtil. Når grenen er tom for levende sessioner,
-- arkiveres den af sit eget fejejob.
--
-- Grenen bliver liggende under priority imens og beholder sine lofter. Det er
-- et bevidst valg: sessionerne står der allerede, og de skal gennemføre. Prisen
-- er, at en nedgradering ikke frigiver kapacitet med det samme.
--
-- Tilstanden er et tidspunkt og ikke et boolsk flag, i samme form som
-- archived_at: null betyder "ikke under dræning", og værdien fortæller hvornår,
-- hvilket er det, man vil vide, når en gren har hængt for længe.
ALTER TABLE queue
    ADD COLUMN draining_at DATETIME(6) NULL;

-- Fejejobbet spørger efter drænende grene hvert interval. Kolonnen er null på
-- stort set alle rækker, så indekset er lille uanset træets størrelse.
CREATE INDEX ix_queue_draining ON queue (draining_at);

-- Fler-pod-låsen for dræningsjobbet. De optagne rækker er aktiveringsmotoren
-- (1, V5), expiry (2, V7), retention (3, V8) og lodtrækningen (4, V11); jobbene
-- serialiseres hver for sig, så et langsomt natjob ikke kan holde motoren ude.
INSERT INTO queue_activation_lock (id) VALUES (5);
