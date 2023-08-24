# Household Budget Survey of Iran (HBSIR) 
<img src='https://github.com/Iran-Open-Data/HBSIR/assets/36173945/af8a7d40-d610-42e2-b6b4-c220f7430df4' align="right" height="139" />
<!-- [![en](https://img.shields.io/badge/lang-en-red.svg)](https://github.com/Iran-Open-Data/HBSIR/blob/main/README.md)
[![en](https://img.shields.io/badge/lang-fa-green.svg)](https://github.com/Iran-Open-Data/HBSIR/blob/main/README.fa.md) -->

**Simplify Your Analysis of Household Budget Survey Data**

Analyzing the Household Budget Survey of Iran (HBSIR) data has historically been challenging due to the constant changes in codings and table schemas, the lack of standardization and alignment with universal coding systems, absence of clear and consistent specifications for data types assigned to variables, and the improper distribution format. With this package, you can seamlessly load, process, and analyze HBSIR data, allowing you to focus on your research and derive insights instead of grappling with these challenges.


## Package Structure
The HBSIR package consists of three core components, each designed to not only streamline your experience working with the HBSIR data but also maximize the reproducibility of results and offer a high degree of flexibility for customizations.

1. Data Extraction:  
This component focuses on extracting data from Microsoft Access files, applying labels, and performing essential data cleaning tasks. Its purpose is to ensure that the data is transformed into a usable format suitable for further analysis.

1. Data Manipulation:  
The package offers a uniform API designed to streamline the process of working with HBSIR data. It simplifies tasks like data loading, adding goods and other classifications, household attributes, and sampling weights.

1. Schema System  
We introduce a schema system that empowers users to define customized table structures based on household budget data.

## Motivation
The motivation behind the HBSIR Data Python Package stems from the recognition of the extreme difficulty faced by users when dealing with HBSIR data. The data's original format in MS Access, coupled with its lack of standardization across different years, posed significant challenges for analysis. This package was conceived to provide a solution that simplifies the entire process – from data extraction to analysis – making HBSIR data more accessible and usable for everyone.

## Installation
Getting started with the package is straightforward. Simply install it using pip:  

```sh
pip install hbsir
```

For those interested in the under-development version, you can also download it as follows:
```sh
pip install git+https://github.com/Iran-Open-Data/HBSIR.git
```

## Quick Usage Example
**Breakdown of food expenditure by urban families in Tehran**  
The purpose of this example is to provide a brief overview of package usage. In the following steps, we will guide you through various stages, such as loading specific tables, and incorporating attributes, classifications, and sampling weights. As we conclude this example, we will have generated a breakdown of food expenses among the urban residents of Tehran. 

```python
# Import the hbsir package
import hbsir

# Load food table for year 1400 into dataframe
food_table = hbsir.load_table("food", 1400) 
```
By employing the provided code, you will read the table into a Pandas DataFrame format.  
Below is an illustration depicting the potential presentation of the loaded table:

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Year</th>
      <th>ID</th>
      <th>Code</th>
      <th>Provision_Method</th>
      <th>Amount</th>
      <th>Duration</th>
      <th>Price</th>
      <th>Expenditure</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1400</td>
      <td>10004004227</td>
      <td>11112</td>
      <td>Purchase</td>
      <td>10.0</td>
      <td>30</td>
      <td>250000.0</td>
      <td>2500000.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1400</td>
      <td>10004004227</td>
      <td>11142</td>
      <td>Purchase</td>
      <td>10.0</td>
      <td>30</td>
      <td>30000.0</td>
      <td>300000.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1400</td>
      <td>10004004227</td>
      <td>11143</td>
      <td>Purchase</td>
      <td>10.0</td>
      <td>30</td>
      <td>30000.0</td>
      <td>300000.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1400</td>
      <td>10004004227</td>
      <td>11174</td>
      <td>Purchase</td>
      <td>2.0</td>
      <td>30</td>
      <td>350000.0</td>
      <td>700000.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1400</td>
      <td>10004004227</td>
      <td>11211</td>
      <td>Purchase</td>
      <td>2.0</td>
      <td>30</td>
      <td>1300000.0</td>
      <td>2600000.0</td>
    </tr>
  </tbody>
</table>


Next, we incorporate the urban-rural and province attributes into the table and apply filtering:
```python
# Drop unnecessary columns 
df = df.drop(columns=["Table_Name", "Provision_Method", "Amount", "Duration"])

# Add urban/rural attribute column
df = hbsir.add_attribute(df, "Urban_Rural")

# Add province attribute column
df = hbsir.add_attribute(df, "Province")

# Filter to only urban Tehran rows
filt = (df["Urban_Rural"] == "Urban") & (df["Province"] == "Tehran")
df = df.loc[filt]
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Year</th>
      <th>ID</th>
      <th>Code</th>
      <th>Price</th>
      <th>Expenditure</th>
      <th>Urban_Rural</th>
      <th>Province</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>107928</th>
      <td>1400</td>
      <td>12313290719</td>
      <td>11144</td>
      <td>25000.0</td>
      <td>125000.0</td>
      <td>Urban</td>
      <td>Tehran</td>
    </tr>
    <tr>
      <th>107929</th>
      <td>1400</td>
      <td>12313290719</td>
      <td>11151</td>
      <td>25000.0</td>
      <td>400000.0</td>
      <td>Urban</td>
      <td>Tehran</td>
    </tr>
    <tr>
      <th>107930</th>
      <td>1400</td>
      <td>12313290719</td>
      <td>11164</td>
      <td>128571.0</td>
      <td>180000.0</td>
      <td>Urban</td>
      <td>Tehran</td>
    </tr>
    <tr>
      <th>107931</th>
      <td>1400</td>
      <td>12313290719</td>
      <td>11165</td>
      <td>140000.0</td>
      <td>70000.0</td>
      <td>Urban</td>
      <td>Tehran</td>
    </tr>
    <tr>
      <th>107932</th>
      <td>1400</td>
      <td>12313290719</td>
      <td>11172</td>
      <td>350000.0</td>
      <td>350000.0</td>
      <td>Urban</td>
      <td>Tehran</td>
    </tr>
  </tbody>
</table>


In the next phase, we'll incorporate classification and sampling weights to compute weighted expenditures.
```python
# Remove unnecessary columns
df = df.drop(columns=["Urban_Rural", "Province"])

# Classify based on food types
df = hbsir.add_classification(df, "original", levels=[2], output_column_names=["Food_Type"])

# Integrate sampling weights
df = hbsir.add_weight(df)

# Compute weighted expenditure
df["Weighted_Expenditure"] = df["Expenditure"] * df["Weight"]
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Year</th>
      <th>ID</th>
      <th>Code</th>
      <th>Price</th>
      <th>Expenditure</th>
      <th>Food_Type</th>
      <th>Weight</th>
      <th>Weighted_Expenditure</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>107928</th>
      <td>1400</td>
      <td>12313290719</td>
      <td>11144</td>
      <td>25000.0</td>
      <td>125000.0</td>
      <td>cereals_and_cereal_products</td>
      <td>2155</td>
      <td>269375008.0</td>
    </tr>
    <tr>
      <th>107929</th>
      <td>1400</td>
      <td>12313290719</td>
      <td>11151</td>
      <td>25000.0</td>
      <td>400000.0</td>
      <td>cereals_and_cereal_products</td>
      <td>2155</td>
      <td>862000000.0</td>
    </tr>
    <tr>
      <th>107930</th>
      <td>1400</td>
      <td>12313290719</td>
      <td>11164</td>
      <td>128571.0</td>
      <td>180000.0</td>
      <td>cereals_and_cereal_products</td>
      <td>2155</td>
      <td>387900000.0</td>
    </tr>
    <tr>
      <th>107931</th>
      <td>1400</td>
      <td>12313290719</td>
      <td>11165</td>
      <td>140000.0</td>
      <td>70000.0</td>
      <td>cereals_and_cereal_products</td>
      <td>2155</td>
      <td>150850000.0</td>
    </tr>
    <tr>
      <th>107932</th>
      <td>1400</td>
      <td>12313290719</td>
      <td>11172</td>
      <td>350000.0</td>
      <td>350000.0</td>
      <td>cereals_and_cereal_products</td>
      <td>2155</td>
      <td>754249984.0</td>
    </tr>
  </tbody>
</table>

Additionally, we will calculate the sum of weights for urban residents of Tehran.
```python
# Load weights table for year 1400
weights = hbsir.load_table("Weights", 1400)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Year</th>
      <th>ID</th>
      <th>Weight</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1400</td>
      <td>10001000226</td>
      <td>1245.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1400</td>
      <td>10001000235</td>
      <td>1245.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1400</td>
      <td>10011009720</td>
      <td>201.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1400</td>
      <td>10011009735</td>
      <td>201.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1400</td>
      <td>10003003235</td>
      <td>237.0</td>
    </tr>
  </tbody>
</table>

If method chaining is your preference, this package provides a pandas extension that facilitates this approach. Pay attention to the subsequent code, which is composed using this method:
```python
# Calculate the sum of weights
weights_sum = (
    weights
    .hbsir.add_attribute(attribute_name="Urban_Rural")
    .hbsir.add_attribute(attribute_name="Province")
    .query("`Urban_Rural`=='Urban' & Province=='Tehran' & Weight.notnull()")
    .sum()
    .loc["Weight"]
)

print(weights_sum)
>>> 4466717.0
```

At this final stage, we proceed to calculate the weighted expenditure associated with each type of food expense.
```python
# Compute the weighted mean
df = df.groupby("Food_Type")[["Weighted_Expenditure"]].sum()
df = df / weights_sum / 1e4

# Calculate the sum of expenditures
df["sum"] = df.sum()

# Rename column and sort results
df = df.rename(columns={"Weighted_Expenditure": "Food_Expenditure (k Tomans)"})
df = df.sort_values("Food_Expenditure (k Tomans)", ascending=False)
```
Outcome of the analysis:

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th>Food_Type</th>
      <th>Food_Expenditure (k Tomans)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>sum</th>
      <td>2,255</td>
    </tr>
    <tr>
      <th>cereals_and_cereal_products</th>
      <td>481</td>
    </tr>
    <tr>
      <th>meat</th>
      <td>425</td>
    </tr>
    <tr>
      <th>fruits_and_nuts</th>
      <td>325</td>
    </tr>
    <tr>
      <th>vegetables_and_pulses</th>
      <td>307</td>
    </tr>
    <tr>
      <th>milk_other_dairy_product_excluding_butter_and_eggs</th>
      <td>268</td>
    </tr>
    <tr>
      <th>sugar_confectionery_and_desserts</th>
      <td>106</td>
    </tr>
    <tr>
      <th>spices_condiments_and_other_food_products</th>
      <td>85</td>
    </tr>
    <tr>
      <th>oils_fats_and_butter</th>
      <td>84</td>
    </tr>
    <tr>
      <th>tea_coffee_and_cocoa_drinks</th>
      <td>69</td>
    </tr>
    <tr>
      <th>soft_drinks</th>
      <td>51</td>
    </tr>
    <tr>
      <th>fish_and_other_sea_foods</th>
      <td>48</td>
    </tr>
  </tbody>
</table>
