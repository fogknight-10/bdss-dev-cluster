# data/

This folder is bind-mounted at `/data` on the `dev` container and on `worker1`-`worker4`, so every file you put
here can be read by Spark as `file:///data/<name>` without copying it into any container.

**Its content is not published with the repository.** `.gitignore` tracks only this README and the `.gitkeep`
placeholder. The sample datasets of the original cluster have their own terms (for example, the MovieLens 100k
README says the data may not be redistributed without separate permission) and the original repository publishes
no license, so they are not republished here.

## Getting the original sample datasets (optional)

```bash
git clone https://github.com/usf-etman/bdss-cluster
cp -r bdss-cluster/volumes/data/* <path-to>/bdss-dev-cluster/data/
```

The quick-start notebook and `scripts/smoke_test.py` do not need them: they create their own small files
(`data/quickstart/`, `data/_bdss_smoke/`).

Repository: https://github.com/fogknight-10/bdss-dev-cluster
