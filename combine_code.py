import os
from collections import defaultdict


def collect_python_files(src_dir, output_file):

    direct_imports = set()
    from_imports = defaultdict(set)
    file_contents = []

    for subdir, _, files in os.walk(src_dir):
        if any(skip in subdir for skip in ["__pycache__", "venv", ".venv", "migrations"]):
            continue

        for file in sorted(files):
            if file.endswith(".py"):
                file_path = os.path.join(subdir, file)
                rel_path = os.path.relpath(file_path, src_dir)

                with open(file_path, "r", encoding="utf-8") as f:
                    lines = f.readlines()

                code_lines = []

                for line in lines:
                    stripped = line.strip()

                    if stripped.startswith("import "):
                        direct_imports.add(stripped)

                    elif stripped.startswith("from ") and " import " in stripped:
                        module, names = stripped.split(" import ")
                        module = module.replace("from ", "").strip()
                        if module.startswith(".") or module.startswith("polydb"):
                            continue
                        for name in names.split(","):
                            from_imports[module].add(name.strip())

                    else:
                        code_lines.append(line)

                file_contents.append((rel_path, code_lines))

    with open(output_file, "w", encoding="utf-8") as outfile:

        outfile.write("# ======================================================\n")
        outfile.write("# Combined Imports\n")
        outfile.write("# ======================================================\n\n")

        # normal imports
        for imp in sorted(direct_imports):
            outfile.write(imp + "\n")

        outfile.write("\n")

        # merged from imports
        for module in sorted(from_imports):
            names = ", ".join(sorted(from_imports[module]))
            outfile.write(f"from {module} import {names}\n")

        outfile.write("\n\n")

        # write code
        for rel_path, code_lines in file_contents:
            outfile.write("# ======================================================\n")
            outfile.write(f"# FILE: {rel_path}\n")
            outfile.write("# ======================================================\n\n")

            outfile.writelines(code_lines)
            outfile.write("\n\n")


if __name__ == "__main__":
    collect_python_files("src", "polydb_code_combined.py")
