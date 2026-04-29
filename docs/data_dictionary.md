# Data Dictionary Template

Use this file to document every important field in your dataset. A strong data dictionary makes your cleaning decisions, KPI logic, and dashboard filters much easier to review.

## How To Use This File

1. Add one row for each column used in analysis or dashboarding.
2. Explain what the field means in plain language.
3. Mention any cleaning or standardization applied.
4. Flag nullable columns, derived fields, and known quality issues.

## Dataset Summary

| Item              | Details                               |
| ----------------- | ------------------------------------- |
| **Dataset Name**  | NYC Rolling Sales Dataset             |
| **Source**        | NYC Department of Finance / Kaggle    |
| **Raw File Name** | cleaned_data.csv                      |
| **Last Updated**  | 2026                                  |
| **Granularity**   | One row per property sale transaction |


## Column Definitions

| Column Name                    | Data Type | Description                            | Example Value                  | Used In             | Cleaning Notes                        |
| ------------------------------ | --------- | -------------------------------------- | ------------------------------ | ------------------- | ------------------------------------- |
| borough                        | int       | Numeric identifier for NYC boroughs    | 1                              | EDA / KPI / Tableau | Mapped borough codes to borough names |
| neighborhood                   | string    | Neighborhood where property is located | CHELSEA                        | EDA / Tableau       | Standardized text formatting          |
| building_class_category        | string    | Property/building category             | 07 RENTALS - WALKUP APARTMENTS | EDA / Tableau       | Removed inconsistent spacing          |
| tax_class_at_present           | string    | Current tax class of property          | 2B                             | EDA                 | Null values handled                   |
| block                          | int       | Property block number                  | 765                            | EDA                 | Type casting applied                  |
| lot                            | int       | Property lot number                    | 25                             | EDA                 | Type casting applied                  |
| building_class_at_present      | string    | Current building class code            | C4                             | EDA / Tableau       | Standardized categorical labels       |
| address                        | string    | Property address                       | 219 WEST 15TH STREET           | Tableau             | Removed duplicate spacing             |
| zip_code                       | int       | ZIP code of property                   | 10011                          | Tableau             | Missing values checked                |
| residential_units              | int       | Number of residential units            | 9                              | KPI / Tableau       | Null values replaced with 0           |
| commercial_units               | int       | Number of commercial units             | 0                              | KPI / Tableau       | Null values replaced with 0           |
| total_units                    | int       | Total number of units                  | 9                              | KPI / Tableau       | Verified unit consistency             |
| land_square_feet               | float     | Land area in square feet               | 1566.0                         | KPI / Tableau       | Missing and zero values handled       |
| gross_square_feet              | float     | Total building area in square feet     | 6330.0                         | KPI / Tableau       | Outliers and zero values checked      |
| year_built                     | int       | Year property was built                | 1901                           | EDA / Tableau       | Invalid years filtered                |
| tax_class_at_time_of_sale      | int       | Tax class during sale                  | 2                              | EDA                 | Type casting applied                  |
| building_class_at_time_of_sale | string    | Building class during sale             | C4                             | Tableau             | Standardized category labels          |
| sale_price                     | float     | Property transaction amount            | 1583840.0                      | KPI / Tableau       | Removed invalid zero-price records    |
| sale_date                      | date      | Date of property sale                  | 2017-08-31                     | EDA / Tableau       | Converted to datetime format          |

## Derived Columns

| Derived Column     | Logic                              | Business Meaning                                   |
| ------------------ | ---------------------------------- | -------------------------------------------------- |
| avg_price_per_sqft | sale_price / gross_square_feet     | Measures property value efficiency by area         |
| building_age       | Current Year - year_built          | Helps analyze age distribution of sold properties  |
| sale_month         | Extracted from sale_date           | Used for monthly trend analysis                    |
| sale_quarter       | Quarter extracted from sale_date   | Used for quarterly transaction analysis            |
| sale_season        | Derived from sale month            | Helps identify seasonal sales patterns             |
| price_category     | Categorized sale_price into ranges | Enables segmentation of low/high priced properties |

## Data Quality Notes

Dataset contains 33,167 property sale transaction records.
Sales transaction dates mainly cover 2016–2017.
Building construction years range from historical periods (1800s/1900s) to modern years.
Null and zero values were identified in gross_square_feet, land_square_feet, and sale_price.
Duplicate records and inconsistent category labels were cleaned during preprocessing.
Outliers in sale_price were reviewed for analysis consistency.
Borough codes were mapped to readable borough names for visualization purposes.
