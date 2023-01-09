import pandas as pd

txt_path = "data/Giva_neigh.txt"
# take from: https://www.givatayim.muni.il/979/


if __name__ == "__main__":
    res_dict = {}

    with open(txt_path, "r", encoding="utf-8") as f:
        neighberhoods = [l.strip() for l in f.readlines() if len(l.strip()) > 0]

        for n_raw in neighberhoods:
            m_idx = n_raw.index("-")
            assert m_idx > 0
            n_name = n_raw[:m_idx].strip()
            n_raw_left = n_raw[m_idx+1:].strip()
            s_names = [s.strip() for s in n_raw_left.split(",")]
            res_dict[n_name] = s_names

    for k,v in res_dict.items():
        print("N: %s" % k)
        print("N: %s" % v)
        print("-----")

    res_df_dict = {"neighberhood" : [], "street": []}
    for k in res_dict.keys():
        for v in res_dict[k]:
            res_df_dict["neighberhood"].append(k)
            res_df_dict["street"].append(v)

    res_df = pd.DataFrame.from_dict(res_df_dict)
    res_df.to_csv("data/Giva_Neighberhoods.csv", index=None)
