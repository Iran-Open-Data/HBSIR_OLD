from pathlib import Path

from matplotlib import pyplot as plt, style

import hbsir

style.use("fivethirtyeight")
plt.xkcd()

FIRST_YEAR = 1376
LAST_YEAR = 1388

plot_data = (
    hbsir.load_table("Expenditures", [FIRST_YEAR, LAST_YEAR])
    .pipe(hbsir.add_attribute, "Urban_Rural")
    .query("Urban_Rural=='Urban'")
    .pipe(hbsir.add_classification, "Food-NonFood")
    .groupby(["Year", "ID", "Food-NonFood"])["Gross_Expenditure"].sum()
    .unstack()
    .rename(columns = {"Non-Food": "NonFood"})
    .assign(Ratio=lambda df : df.eval("Food / (Food + NonFood) * 100"))
    .dropna(subset="Ratio")
    .pipe(hbsir.add_weight)
    .assign(Poor=lambda df : df.eval("(Ratio >= 40) * Weight"))
    .pipe(hbsir.add_attribute, "Province")
    .groupby(["Year", "Province"])[["Poor", "Weight"]].sum()
    .eval("Poor / Weight * 100")
    .unstack(0)
    .rename(columns={FIRST_YEAR: str(FIRST_YEAR), LAST_YEAR: str(LAST_YEAR)})
    .eval(f"diff = `{FIRST_YEAR}` - `{LAST_YEAR}`")
    .sort_values("diff", ascending=False)
    .dropna()
)


fig, ax = plt.subplots(figsize=(8, 10))

ax.scatter(plot_data[str(FIRST_YEAR)], plot_data.index, label=FIRST_YEAR)
ax.scatter(plot_data[str(LAST_YEAR)], plot_data.index, label=LAST_YEAR)

for i, row in plot_data.iterrows():
    if row["diff"] >= 3:
        ax.annotate(
            text = "",
            xy = (
                row[str(LAST_YEAR)] + 0.5,
                i,
            ),
            xytext = (
                row[str(FIRST_YEAR)] - 0.6,
                i,
            ),
            arrowprops = dict(
                color = "green",
                arrowstyle = "-|>"
            )
        )

    if row["diff"] <= -3:
        ax.annotate(
            text = "",
            xy = (
                row[str(LAST_YEAR)] - 0.5,
                i,
            ),
            xytext = (
                row[str(FIRST_YEAR)] + 0.6,
                i,
            ),
            arrowprops = dict(
                color = "red",
                arrowstyle = "-|>"
            )
        )

fig.suptitle(f"Provincial Poverty* Rate Changes {FIRST_YEAR} - {LAST_YEAR}")
ax.set_xlabel("Poverty Rate (%)")
ax.legend()

fig.text(
    0.06,
    0.02,
    r"*Poor: A household whose share of food, exceeds 40% of its total expenditures.",
    fontdict={"size": 10}
)

fig.subplots_adjust(left=0.38, bottom=0.12, top=0.92)
path = Path(__file__).parent.joinpath(f"{FIRST_YEAR}-{LAST_YEAR}.png")
fig.savefig(str(path), format="png")
