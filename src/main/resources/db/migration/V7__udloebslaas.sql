-- Endnu en singleton-række i motorens låse-tabel: id 2 serialiserer
-- expiry-jobbet (EXPIRED/ABANDONED-oprydning) på tværs af pods — samme
-- skip-locked-mønster som aktiveringsmotoren (id 1), men uafhængigt af den.

INSERT INTO queue_activation_lock (id) VALUES (2);
