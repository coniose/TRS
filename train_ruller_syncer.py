import pandas as pd
import pprint
import json

import numpy as np

pp = pprint.PrettyPrinter(indent=4)
df_name = "tid_mvp.xlsx"

# with pd.ExcelWriter("tid_mvp.xlsx") as writer:
#     df_colaboradores.to_excel(writer, sheet_name="colaboradores")
#     df_treino_info.to_excel(writer, sheet_name="treinamentos_info")
#     df_grade_cargos.to_excel(writer, sheet_name="grade_ruller")


def df_api_connector():
    json_grade_file = json.load(open("grade.json", encoding="utf8"))

    return json_grade_file[0], json_grade_file[1]


grade_treinos, grade_cargos = df_api_connector()

df_grade_cargos = pd.DataFrame(grade_cargos["cargos"])

df_treino_info = pd.DataFrame(grade_treinos["treinamentos"])

df_colaboradores = pd.read_excel(df_name, sheet_name="colaboradores")


class Whisper:
    def __init__(self, df_grade_cargos, df_treino_info, df_colaboradores):
        self._df_grade_cargos = pd.DataFrame(df_grade_cargos)
        self._df_treino_info = pd.DataFrame(df_treino_info)
        self._df_colaboradores = pd.DataFrame(df_colaboradores)

    def df_relater(self):
        df_grade_cargos = self._df_grade_cargos.copy()
        df_treino_info = self._df_treino_info.copy()
        df_colaboradores = self._df_colaboradores.copy()

        colab_grade_merge = pd.merge(
            df_colaboradores, df_grade_cargos, on=["cargo", "area"]
        ).rename(
            columns={
                "treinamentos_id_x": "treinamentos_id",
                "treinamentos_id_y": "treinamentos_id_required",
            }
        )
        colab_grade_info_merge = pd.merge(
            colab_grade_merge, df_treino_info, on=["treinamentos_id", "treinamentos_id"]
        )
        return colab_grade_info_merge

    def delay_whisper(self):
        df = self.df_relater().copy()
        df["data_expiracao"] = df.apply(
            lambda row: pd.Timestamp(row["data_treinamento"])
            + pd.Timedelta(days=row["validade"]),
            axis=1,
        )
        return df

    def missing_whisper(self):
        df = self.delay_whisper().copy()
        df["treinamentos_id"] = df["treinamentos_id"].astype(int)
        df_grouped_missing = df.groupby(["login", "cargo", "area"]).apply(
            lambda x: pd.Series(
                {
                    "treinamentos_id": x["treinamentos_id"].tolist(),
                    "treinamentos_id_required": x["treinamentos_id_required"].tolist()[
                        0
                    ],
                }
            )
        )

        required = df_grouped_missing["treinamentos_id_required"].values
        completed = df_grouped_missing["treinamentos_id"].values

        faltantes = [
            np.setdiff1d(req, comp).tolist() for req, comp in zip(required, completed)
        ]
        df_grouped_missing["treinamentos_faltantes"] = faltantes

        return df_grouped_missing

    def outdated_whisper(self):
        df = self.delay_whisper().copy()
        df["treinamentos_id"] = df["treinamentos_id"].astype(int)
        df["hoje"] = [pd.Timestamp.today() for line in range(len(df))]
        hoje = np.array(df["hoje"].values, dtype="datetime64[D]")
        data_expiracao = np.array(df["data_expiracao"], dtype="datetime64[D]")
        df["dias_expirados"] = (hoje - data_expiracao).astype(int)

        df["treinamentos_expirados"] = [
            expirado if dias >= 0 else None
            for expirado, dias in zip(df["treinamentos_id"], df["dias_expirados"])
        ]

        return df

    def outdated_missing_whisper(self):
        missing = self.missing_whisper()
        outdated = self.outdated_whisper()
        df_merged = pd.merge(missing, outdated, on=["login", "cargo"])
        return df_merged


whisper_builder = Whisper(df_grade_cargos, df_treino_info, df_colaboradores)
# result = whisper_builder.outdated_whisper()
# result = whisper_builder.missing_whisper()
result = whisper_builder.outdated_missing_whisper()
pp.pprint(result)
# result.to_excel("all.xlsx")
