import ast
import argparse
import json
from pathlib import Path


class PythonArchitectureExtractor(ast.NodeVisitor):

    def __init__(self, ignore_private=True):
        self.classes = {}
        self.ignore_private = ignore_private

    def visit_ClassDef(self, node):

        methods = []

        for item in node.body:
            if isinstance(item, ast.FunctionDef):

                name = item.name

                if self.ignore_private and name.startswith("_"):
                    continue

                params = []

                for arg in item.args.args:
                    if arg.arg != "self":
                        params.append(arg.arg)

                param_str = ", ".join(params)

                return_type = ""

                if item.returns:
                    try:
                        return_type = " -> " + ast.unparse(item.returns)
                    except Exception:
                        pass

                signature = f"{name}({param_str}){return_type}"

                methods.append(signature)

        if methods:
            self.classes[node.name] = methods

        self.generic_visit(node)


def parse_file(path: Path, ignore_private=True):

    try:

        source = path.read_text(encoding="utf-8")

        tree = ast.parse(source)

        extractor = PythonArchitectureExtractor(ignore_private)

        extractor.visit(tree)

        return extractor.classes

    except Exception:
        return {}


def scan_directory(directory: str, ignore_private=True):

    results = {}

    for file in Path(directory).rglob("*.py"):

        classes = parse_file(file, ignore_private)

        if classes:
            results[str(file)] = classes

    return results


def generate_text(classes_by_file):

    lines = []

    for file, classes in classes_by_file.items():

        lines.append(f"\nFILE: {file}\n")

        for cls, methods in classes.items():

            lines.append(cls)

            for m in methods:
                lines.append(f"  ├─ {m}")

            lines.append("")

    return "\n".join(lines)


def generate_markdown(classes_by_file):

    lines = ["# Architecture Overview\n"]

    for file, classes in classes_by_file.items():

        lines.append(f"## {file}\n")

        for cls, methods in classes.items():

            lines.append(f"### {cls}")

            for m in methods:
                lines.append(f"- `{m}`")

            lines.append("")

    return "\n".join(lines)


def generate_mermaid(classes_by_file):

    lines = ["```mermaid", "classDiagram"]

    for classes in classes_by_file.values():

        for cls, methods in classes.items():

            lines.append(f"class {cls} {{")

            for m in methods:
                lines.append(f"  +{m}")

            lines.append("}")

    lines.append("```")

    return "\n".join(lines)


def save_output(content: str, output_path: Path):

    output_path.parent.mkdir(parents=True, exist_ok=True)

    output_path.write_text(content, encoding="utf-8")

    print(f"\nArchitecture written to: {output_path}\n")


def main():

    parser = argparse.ArgumentParser(description="Python Architecture Extractor")

    parser.add_argument("--src", default="src", help="Source directory (default: src)")

    parser.add_argument("--format", default="text", choices=["text", "markdown", "mermaid", "json"])

    parser.add_argument(
        "--output", default="architecture_output", help="Output file name (without extension)"
    )

    parser.add_argument("--include-private", action="store_true")

    args = parser.parse_args()

    classes = scan_directory(args.src, ignore_private=not args.include_private)

    if args.format == "text":
        content = generate_text(classes)
        extension = "txt"

    elif args.format == "markdown":
        content = generate_markdown(classes)
        extension = "md"

    elif args.format == "mermaid":
        content = generate_mermaid(classes)
        extension = "md"

    elif args.format == "json":
        content = json.dumps(classes, indent=2)
        extension = "json"

    output_file = Path("architecture") / f"{args.output}.{extension}"

    save_output(content, output_file)


if __name__ == "__main__":
    main()
