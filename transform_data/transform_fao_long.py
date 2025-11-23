# transform_data/transform_fao_long.py
import pandas as pd
import os

def transform_fao(input_file=None, output_dir="output_data/fao_long", **kwargs):
    """
    Pull input_file path from XCom if not provided,
    transform CSV to long format,
    push output file path to XCom
    """
    if input_file is None:
        ti = kwargs['ti']
        input_file = ti.xcom_pull(task_ids="extract_fao")  # pull from previous task

    df = pd.read_csv(input_file)

    # Lấy các cột năm
    year_cols = [c for c in df.columns if c[:4].isdigit()]

    # UNPIVOT
    df_long = pd.melt(
        df,
        id_vars=["Country Name En", "Unit Name"],
        value_vars=year_cols,
        var_name="year",
        value_name="value"
    )

    df_long.rename(columns={
        "Country Name En": "country",
        "Unit Name": "unit"
    }, inplace=True)

    df_long["year"] = df_long["year"].astype(int)
    df_long["value"] = pd.to_numeric(df_long["value"], errors="coerce")
    df_long.dropna(subset=["value"], inplace=True)

    os.makedirs(output_dir, exist_ok=True)
    out_file = os.path.join(output_dir, "fao_long.csv")
    df_long.to_csv(out_file, index=False)
    print(f"✅ Transform xong: {out_file}")
    return out_file  # push to XCom

if __name__ == "__main__":
    csv_input = "data_input/data_fao/fao_demo.csv"
    output_dir = "output_data/fao_long"
    csv_file = transform_fao(input_file=csv_input, output_dir=output_dir)
    print(f"✅ CSV transformed: {csv_file}")
