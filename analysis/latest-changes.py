import subprocess

# Run git diff with specific options
result = subprocess.run(
    ["git", "diff", "--unified=0"], 
    capture_output=True, 
    text=True, 
    encoding='latin-1'  
)

# Filter only added (+) and removed (-) lines
diff_lines = [line for line in result.stdout.split("\n") if line.startswith(("+")) and not line.startswith(("+++"))]

# Print or process the diff lines
for line in diff_lines:
    cleaned_line = line[1:].strip()
    parts = cleaned_line.split(",")
    if len(parts) > 1:
       print(parts[1].strip())  # Strip to remove leading/trailing spaces
