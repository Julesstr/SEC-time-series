# SEC Time Series Data

This project serves to facilitate comparison of quarterly financial statement and notes data filed by US companies with the SEC between 2009 and the present with nearly unlimited companies and data tags supported simultaneously. See the [SEC & Markets Data](https://www.sec.gov/data-research/sec-markets-data/financial-statement-notes-data-sets) page for further details.

## Database

The main.py file creates a database with all the necessary information for comparing accounting tags across companies, excluding non-standard tags by default for memory optimisation. This database can then be queried with any list of valid Central Index Keys (CIKs) and tags, or all of either.
The resulting data are formatted with stacked CIKs and unstacked tags:

|   cik   |   ddate    |     name       |    Assets    | Liabilities  |
|--------:|:-----------|:----------------|-------------:|-------------:|
| 320193  | 2024-06-30 | APPLE INC       | 3.316e+11     | 2.649e+11     |
| 320193  | 2024-09-30 | APPLE INC       | 3.650e+11     | 3.080e+11     |
| 789019  | 2024-06-30 | MICROSOFT CORP  | 5.122e+11     | 2.437e+11     |
| 789019  | 2024-09-30 | MICROSOFT CORP  | 5.230e+11     | 2.353e+11     |


You can find Documentation for the downloaded files [here](https://www.sec.gov/files/financial-statement-data-sets.pdf), and relevant US GAAP XBRL taxomony for tags [here](https://fasb.org/page/detail?pageId=/projects/FASB-Taxonomies/2025-disclaimer-gaap-financial-reporting-taxonomy.html) (2025).


## Disclaimer

XBRL tags are not easy to compare. Tag usage is inconsistent within and across companies. As far as I know, standardisation models exist only as property of data providers.





