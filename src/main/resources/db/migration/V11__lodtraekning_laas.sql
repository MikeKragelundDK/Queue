-- Fler-pod-lås til lodtrækningsjobbet. Samme mønster som de øvrige jobs:
-- rækken indeholder ikke andet end sit id og findes udelukkende for at kunne
-- låses med "select ... for update skip locked".
INSERT INTO queue_activation_lock (id) VALUES (4);
