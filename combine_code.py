import os

def collect_python_files(src_dir, output_file):
    with open(output_file, "w", encoding="utf-8") as outfile:
        for subdir, _, files in os.walk(src_dir):
            # skip unwanted directories
            if any(
                skip in subdir
                for skip in ["__pycache__", "venv", ".venv", "migrations"]
            ):
                continue
            for file in files:
                if file.endswith(".py"):
                    file_path = os.path.join(subdir, file)
                    try:
                        with open(file_path, "r", encoding="utf-8") as infile:
                            outfile.write(f"\n\n# === File: {file_path} ===\n\n")
                            outfile.write(infile.read())
                    except Exception as e:
                        print(f"⚠️ Could not read {file_path}: {e}")


if __name__ == "__main__":
    src_dir = "src"  # only read from ./src
    output_file = "all_src_code_combined.py"
    collect_python_files(src_dir, output_file)
    print(f"✅ Combined Python code from {src_dir}/ saved to {output_file}")
