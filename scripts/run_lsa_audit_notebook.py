#!/usr/bin/env python3
"""Execute the audit notebook sequentially with Python, preserving stdout evidence."""
import argparse,contextlib,hashlib,io,json
from pathlib import Path

def run(path):
    notebook=json.loads(path.read_text());namespace={};records=[]
    for index,cell in enumerate(notebook['cells']):
        if cell['cell_type']!='code':continue
        output=io.StringIO()
        with contextlib.redirect_stdout(output):exec(compile(''.join(cell['source']),f'notebook_cell_{index}','exec'),namespace)
        cell['execution_count']=len(records)+1
        cell['outputs']=[{'output_type':'stream','name':'stdout','text':output.getvalue().splitlines(True)}] if output.getvalue() else []
        records.append({'execution_count':cell['execution_count'],'cell_index':index,'stdout':output.getvalue()})
    path.write_text(json.dumps(notebook,ensure_ascii=False,indent=1)+'\n')
    result={'runner':'Python exec with sequential shared namespace; no Jupyter kernel runner installed','executed_code_cells':len(records),'cells':records,'notebook_sha256':hashlib.sha256(path.read_bytes()).hexdigest()}
    (namespace['OUT']/'notebook_execution.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(f'Executed {len(records)} notebook cells successfully.')

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--notebook',type=Path,default=Path('analysis/lsa_git_audit.ipynb'));run(p.parse_args().notebook)
