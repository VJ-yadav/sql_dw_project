import os

structure = {
    "sql-dw-project": {
        "datasets": "",
        "docs": {
            "etl.drawio": "",
            "data_architecture.drawio": "",
        },
        "scripts": {
            "bronze": {"test.py": ""},
            "silver": {"test.py": ""},
            "gold": {"test.py": ""},
        },
        "tests": {"test.py": ""},
        "requirements.txt": "",
        ".gitignore": "",
        "README.md": "",
    }
}


# add_structure = {}
# def add_structure(base_path, add_structure: dict= {}):
#     pass


def create_project_structure(base_path, structure: dict = {}):
    for name, content in structure.items():
        path = os.path.join(base_path, name)
        if isinstance(content, dict):
            os.makedirs(path, exist_ok=True)
            create_project_structure(path, content)
        else:
            with open(path, "w") as f:
                f.write(content)


create_project_structure(".", structure)
print("Folder structure created successfully!")
