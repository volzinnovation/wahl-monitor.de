# Reproduction

Run `python3 scripts/render_lsa_demographic_tweets.py` from the repository root with Matplotlib, NumPy, pandas and Pillow installed. The exporter reads only the retained demographic pilot and its frozen election snapshot. It does not refresh data.

Eight 1600 × 1000 PNGs correspond to tweets.md and tweets.json in increasing editorial priority. Alt text and input hashes are retained. All 218 municipalities have equal weight in correlations; education has 54 observations. Spearman statistics and their 215/51-row sensitivity checks are independently recalculated from plotted data. No fitted line, confidence interval, causal claim, or individual voting inference is drawn. Population charts intentionally share a logarithmic x-axis; education panels share both axis ranges.

The one-root palette is neutral blue for this nonpartisan series. Direct labels and filled/open markers convey identity without depending on color. Repeated scatter charts answer the same party/feature association question using separate fields. Tweet 8 uses paired dots for a sensitivity comparison, not uncertainty intervals.
