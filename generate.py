#!/bin/env python3

import sys
import json
import os
import random
import re
import time

import pandas as pd
import requests


def main(n, output=None, seed=17, max_id=400_000_000, delay=0.8):
    random.seed(seed)

    auth = (os.environ["GITHUB_USER"], os.environ["GITHUB_PASSWD"])

    generated = []

    data = pd.DataFrame(
        columns=(
            "index",
            "id",
            "owner",
            "owner_type",
            "name",
            "description",
            "fork",
            "created_at",
            "updated_at",
            "homepage",
            "size",
            "stars",
            "watches",
            "has_issues",
            "has_projects",
            "has_downloads",
            "has_wiki",
            "has_pages",
            "language",
            "archived",
            "license",
            "forks",
            "open_issues",
            "commits",
            "readme_len",
        )
    )

    i = 0
    for count in range(n):
        while True:
            while True:
                id = random.randrange(max_id)
                if not id in generated:
                    break
            generated.append(id)

            print(f"Fetch repo #{i} id={id}... ", end="")
            file = f"build/{i}.json"
            if not os.path.exists(file):
                with requests.get(
                    f"https://api.github.com/repositories/{id}",
                    headers={"Accept": "application/vnd.github.v3+json"},
                    auth=auth,
                ) as r, open(file, mode="w") as f:
                    f.write(r.text)

                time.sleep(delay)
            i += 1

            with open(file) as f:
                try:
                    j = json.load(f)
                except json.decoder.JSONDecodeError:
                    print("empty")
                    continue

                if "message" in j:
                    print(j["message"])
                    if not j["message"] in ["Not Found", "Repository access blocked"]:
                        os.remove(file)
                        exit(1)
                    continue

                print(f"found {count+1}/{n}")

                print(f"Fetch commits... ", end="")
                cfile = f"build/{i-1}-commits.txt"
                if not os.path.exists(cfile):
                    with requests.get(
                        f"https://api.github.com/repos/{j['owner']['login']}/{j['name']}/commits?per_page=1",
                        auth=auth,
                    ) as r:
                        cj = r.json()
                        if "message" in cj:
                            if cj["message"] in [
                                "Not Found",
                                "Git Repository is empty.",
                            ]:
                                print("empty repo")
                                with open(cfile, mode="w") as f:
                                    f.write("0")
                            else:
                                print(cj["message"])
                                exit(1)
                        else:
                            print("ok")

                            with open(cfile, mode="w") as f:
                                f.write(
                                    re.search(
                                        r"page=(\d+)", r.links["last"]["url"]
                                    ).group(1)
                                )

                    time.sleep(delay)
                else:
                    print("skipped")

                print(f"Fetch readme... ", end="")
                rfile = f"build/{i-1}-readme.md"
                if not os.path.exists(rfile):
                    with requests.get(
                        f"https://raw.githubusercontent.com/{j['owner']['login']}/{j['name']}/HEAD/README.md",
                    ) as r:
                        if r.status_code == 404:
                            print("none")
                            with open(rfile, mode="w") as f:
                                pass
                        elif r.status_code != 200:
                            print(r.status_code)
                            exit(1)
                        else:
                            print("found")
                            with open(rfile, mode="w") as f:
                                f.write(r.text)

                    time.sleep(delay)
                else:
                    print("skipped")

                break

        with open(cfile) as f:
            cs = f.read()

        with open(rfile) as f:
            rl = f.read()

        data = data.append(
            {
                "index": i - 1,
                "id": id,
                "owner": j["owner"]["login"],
                "owner_type": j["owner"]["type"],
                "name": j["name"],
                "description": j["description"] or "",
                "fork": j["fork"],
                "created_at": j["created_at"],
                "updated_at": j["updated_at"],
                "homepage": j["homepage"] or "",
                "size": j["size"],
                "stars": j["stargazers_count"],
                "watches": j["watchers_count"],
                "has_issues": j["has_issues"],
                "has_projects": j["has_projects"],
                "has_downloads": j["has_downloads"],
                "has_wiki": j["has_wiki"],
                "has_pages": j["has_pages"],
                "language": j["language"] or "",
                "archived": j["archived"],
                "license": j["license"] and j["license"]["spdx_id"] or "",
                "forks": j["forks"],
                "open_issues": j["open_issues"],
                "commits": cs,
                "readme_len": len(rl),
            },
            ignore_index=True,
        )

    if output is not None:
        data.to_csv(output, index=False)
    print(data)


if __name__ == "__main__":
    main(10000, sys.argv[1] if len(sys.argv) > 1 else None)
