# /workspace

Your code. This folder is a bind mount of `./workspace` on your computer and appears at the **same path
`/workspace` on the dev container, worker1-4 and the Jupyter worker**, so anything you save here is visible to
Spark executors without copying.

| Folder | Purpose |
|---|---|
| `notebooks/` | Jupyter notebooks (also mounted into worker4 at `/home/user/notebooks`, next to the Jupyter Notebook on `http://localhost:8000`) |
| `jobs/` | Scripts for `spark-submit`, e.g. `wordcount.py` (counts the words of any text file you pass as argument) |

Quick start: open `notebooks/00_quickstart.ipynb` in VS Code and run all cells.
