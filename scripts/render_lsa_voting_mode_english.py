#!/usr/bin/env python3
"""Render an English translation of chart 11 from the frozen LSA report data."""
import argparse
import csv
import hashlib
import json
from pathlib import Path

from render_lsa_tweet_report import Report, BLUE, ORANGE, plt


def render(source, output):
    report = Report(source)
    output.mkdir(parents=True, exist_ok=True)
    report.footer = (
        f"Source: official CSV/HTML in Git archive {report.s['ref'][:8]} · "
        f"Captured {report.cutoff:%d %b %Y, %H:%M} CEST · wahl-monitor.de"
    )
    in_person = report.raw['lsa:LAND:15:U']
    postal = report.raw['lsa:LAND:15:B']
    fig = report.figure(
        'In-person and postal voting: different party shares',
        f"Valid party-list votes: in person {in_person['valid_votes_zweit']:,} · "
        f"postal {postal['valid_votes_zweit']:,} · separate denominators",
    )
    ax = fig.add_axes([.15, .20, .77, .57])
    a = [report.percent(in_person, code) for code in report.codes]
    b = [report.percent(postal, code) for code in report.codes]
    labels = [{'GRÜNE': 'Greens', 'Die Linke': 'The Left'}.get(name, name) for name in report.names]
    for index, (x, y) in enumerate(zip(a, b)):
        ax.plot([x, y], [index, index], color='#b7bec6', lw=2)
        ax.text(x + .65, index - .10, f'{x:.2f}', color=BLUE, fontsize=11)
        ax.text(y + .65, index + .25, f'{y:.2f}', color=ORANGE, fontsize=11)
    ax.scatter(a, range(len(a)), s=80, color=BLUE, label='In person', zorder=3)
    ax.scatter(b, range(len(b)), s=75, facecolors='white', edgecolors=ORANGE,
               linewidths=2, marker='s', label='Postal', zorder=3)
    ax.set_yticks(range(len(a)), labels)
    ax.invert_yaxis()
    ax.set_xlim(0, max(a + b) + 7)
    ax.set_xlabel('Share of valid party-list votes within each voting method (%)')
    ax.grid(axis='x', alpha=.15)
    ax.legend(frameon=False, ncol=2, loc='lower right')
    target = output / '11_urne_brief.png'
    fig.savefig(target, dpi=140, facecolor='white', metadata={'Software': 'wahl-monitor reproducible audit'})
    plt.close(fig)

    data = []
    for official, english, code in zip(report.names, labels, report.codes):
        for method, row in [('In person', in_person), ('Postal', postal)]:
            data.append({'party': official, 'english_label': english, 'voting_method': method,
                         'party_list_votes': row['parties'][code],
                         'valid_party_list_votes': row['valid_votes_zweit'],
                         'share_percent': report.percent(row, code)})
    with (output / '11_urne_brief.csv').open('w', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=list(data[0]))
        writer.writeheader()
        writer.writerows(data)
    manifest = {'source_git_ref': report.s['ref'], 'capture_local': report.cutoff.isoformat(),
                'translation': 'English; party-list votes means Zweitstimmen',
                'source_sha256': hashlib.sha256((source / 'latest_official_rows.json').read_bytes()).hexdigest(),
                'renderer_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                'files': {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                          for path in [target, output / '11_urne_brief.csv']}}
    (output / 'manifest.json').write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + '\n')
    print(target.resolve())


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, default=Path('data/2026-lsa/reports/git-timeline/80fa3052'))
    parser.add_argument('--output', type=Path, default=Path('data/2026-lsa/reports/git-timeline/translations-80fa3052/en'))
    args = parser.parse_args()
    render(args.input, args.output)
