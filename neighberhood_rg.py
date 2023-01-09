import pandas as pd

txt_path = "data/RG_neigh.txt"

if __name__ == "__main__":
    res_df_dict = {"neighberhood" : [], "street": []}

    with open(txt_path, "r", encoding="utf-8") as f:
        neighberhood = None
        for line in f.readlines():
            line = line.strip()
            if line.startswith("שכונת"):
                neighberhood = line.replace("שכונת", "").strip()
            elif line.startswith("רחוב"):
                street = line.replace("רחוב","").strip()
                res_df_dict['neighberhood'].append(neighberhood)
                res_df_dict['street'].append(street)
            elif len(line) == 0:
                continue
            else:
                raise Exception("Unexpected line: %s" % line)

    res_df = pd.DataFrame.from_dict(res_df_dict)
    res_df.to_csv("data/RG_Neighberhoods.csv", index=None)

