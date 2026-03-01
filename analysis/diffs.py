import git
import pandas as pd
import io

def get_csv_from_commit(repo, commit, file_path):
    """
    Load CSV file content from a given commit as a pandas DataFrame.
    """
    try:
        blob = commit.tree / file_path
        content = blob.data_stream.read().decode('latin-1')
        return pd.read_csv(io.StringIO(content))
    except Exception as e:
        print(f"Error reading {file_path} from commit {commit.hexsha}: {e}")
        return None

def compare_dataframes(df_old, df_new):
    """
    Compare two DataFrames cell by cell for numeric values.
    Ignores changes where the old value is empty.
    Returns a list of changes as dictionaries:
      { 'row': row index, 'column': column name, 'old_value': value, 'new_value': value }
    """
    changes = []
    common_columns = set(df_old.columns) & set(df_new.columns)
    min_rows = min(len(df_old), len(df_new))
    for idx in range(min_rows):
        for col in common_columns:
            old_cell = df_old.iloc[idx][col]
            new_cell = df_new.iloc[idx][col]
            
            # Ignore change if the old cell is empty (NaN or empty string)
            if pd.isna(old_cell) or str(old_cell).strip() == "":
                continue
            old_val = old_cell
            new_val = new_cell
            # try:
            #    old_val = float(old_cell)
            #    new_val = float(new_cell)
            # except (ValueError, TypeError):
            #    continue  # Skip non-numeric cells

            if old_val != new_val:
                changes.append({
                    'row': idx,
                    'city' : df_new.iloc[idx,1],
                    'column': col,
                    'old_value': old_val,
                    'new_value': new_val
                })
    return changes

def main(repo_path, csv_file):
    repo = git.Repo(repo_path)
    commits = list(repo.iter_commits(paths=csv_file))
    commits.reverse()  # chronological order
    
    if len(commits) < 2:
        print("Not enough commits to compare changes.")
        return

    for i in range(1, len(commits)):
        commit_old = commits[i-1]
        commit_new = commits[i]
        df_old = get_csv_from_commit(repo, commit_old, csv_file)
        df_new = get_csv_from_commit(repo, commit_new, csv_file)
        
        if df_old is None or df_new is None:
            continue
        
        changes = compare_dataframes(df_old, df_new)
        if changes:
            timestamp = commit_new.committed_datetime.strftime('%Y-%m-%d %H:%M:%S')
            print(f"Timestamp: {timestamp}")
            for change in changes:
                print(f"City: {change['city']}, Column: {change['column']}, "
                      f"{change['old_value']} -> {change['new_value']}")
            print("-" * 40)

if __name__ == "__main__":
    repo_path = ".."      # Repository path (current directory)
    csv_file1 = "data/latest-btw25-bw-kreise.csv"  # Replace with your CSV file name
    csv_file2 = "data/latest-btw25-bw-gemeinden.csv"  # Replace with your CSV file name
    main(repo_path, csv_file1)
    main(repo_path, csv_file2)