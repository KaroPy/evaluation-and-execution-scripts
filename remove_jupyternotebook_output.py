import nbformat as nb
import glob


def remove_output_cells(notebook_path):
    # Load the Jupyter notebook
    with open(notebook_path, "r") as f:
        notebook = nb.read(f, as_version=4)

    # Remove all output cells
    for icell, cell in enumerate(notebook.cells):
        # print(f"cell = {cell}")
        if cell["cell_type"] == "code":
            if "outputs" in cell.keys():
                # print(notebook.cells[icell]["outputs"])
                notebook.cells[icell]["outputs"] = []

    # Save the modified notebook
    with open(notebook_path, "w") as f:
        nb.write(notebook, f)


def get_all_notebook_files():
    return glob.glob("**/*.ipynb", recursive=True)


def main():
    notebook_files = get_all_notebook_files()
    ignored_files = []
    print(f"notebook_files = {len(notebook_files)}, {notebook_files[0:3]}")
    for ifile, notebook_file in enumerate(notebook_files):
        print(f"file = {notebook_file}")
        try:
            remove_output_cells(notebook_file)
        except Exception as e:
            print("... IGNORE")
            ignored_files.append(notebook_file)
            continue
        # if ifile == 4:
        #     break
    for i in ignored_files:
        print(i)


main()
