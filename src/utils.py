import logging


def write_df_to_csv(df, folder, file_name):
    folder.mkdir(exist_ok=True)
    try:
        df.coalesce(1).write.csv(f"{folder}/{file_name}", mode="overwrite", header=True)
        logging.info(f"Succesfully wrote {file_name}")
    except Exception as e:
        logging.error(f"Failed to write {file_name}: {e}")


def clean_folder(folder):
    for file_path in folder.rglob("*"):
        if file_path.name.startswith("_") or file_path.suffix == ".crc":
            file_path.unlink()


def rename_files(folder):
    for csv_file in folder.rglob("*.csv"):
        folder = "/".join(str(csv_file).split("/")[:-1])
        proper_file_name = f'{"".join(str(csv_file).split("/")[-2])}.csv'
        full_path = f"{folder}/{proper_file_name}"
        csv_file.rename(full_path)
