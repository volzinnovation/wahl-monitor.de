# Frozen published BW and RLP pages

`bw-rlp.tar.gz` contains exact HTTP response bytes copied from the public
`https://wahl-monitor.de/2026-bw/` and `/2026-rlp/` trees on 2026-09-06.
It is a preservation snapshot, not a regeneration from election results.
The source deployment was commit `72c07a260239243921d218836bd844b44c19b241`.

The capture enumerated both published search indexes and the published sitemap,
then followed in-scope HTML asset/page links and literal JavaScript fetch URLs.
All 22,890 files were fetched successfully. `manifest.json` records each file's
SHA-256, byte length, capture time, source deployment, and archive checksum.

The previous GitHub Pages artifact had expired, and existing local generated
pages differed from the live pages. Copying the live responses preserves the
actual published content without rerunning either election's generator.

`scripts/build_lsa_pages.py` restores and verifies this snapshot, builds LSA only,
then checks every frozen file again. It also fingerprints the tracked BW/RLP
result trees and configs before and after the build. A changed, added, missing,
or corrupted frozen file prevents deployment.

Do not regenerate or replace this archive during LSA collection. A future
intentional update to BW/RLP requires a separately reviewed replacement snapshot.
