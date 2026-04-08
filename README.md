## defined-schema-migration
---
Ingestion logic for particular self-storage data

### This Repo:
- It's a simple migration tool designed to ingest .csv data into a defined relational schema.
- This is the schema's ERD (I called `monument`):

![ERD](erd.png)

My solution's [presentation.](https://docs.google.com/presentation/d/1f2DKLG761_9y4sgcUJ5ZcAgRZF1JEmjwVkWaHwHr9yg/edit?slide=id.p#slide=id.p)

### Setup to run the migration:

**Manual setup:**
1. Need an instance of PostgreSQL running, a database with a `monument` and a `monument_raw` schema already created to run the script against.
2. Run the DDL scripts (`sql/ddl.sql`) to create the tables in the PostgreSQL database.
3. Need to create an `.env` file on the root with the variable:
```bash
DATABASE_URL = 'postgresql://$username:$password@$host:$port/$database'
```
You will need to provide `$variables` in order to make the connection run

4. Make sure you have python installed (3.12.9 preferred)
5. Make sure you're in the virtual environment:
```bash
# Windows/macOS/Linux create .venv: 
python -m venv .venv

# Windows activate .venv:
.venv\Scripts\activate

# macOS/Linux activate .venv:
source .venv/bin/activate
```
6. Make sure you have `pip` installed to install the dependencies:
```bash
pip install -r requirements.txt
```

7. With you setup done, you should be able to run the migration tool with:
```bash
python .\src\main.py
```

### Issues:
- Invoice data seems incomplete, the rules for `invoiceAmount` were defined with the available data.
- Uncertainties regarding the data load frequency. Depending on other sources or requirements, the process may not be attend an ACID standard.


### Assumptions made:
- Although `SQLAlchemy` and `Pandas` doesn't offer many security and scripting robustness, is a great tool for PoC's and what I chose to show my desing. 
- I assumed that the `unit.csv` may have several extra records, instead of +300 .csv files, and the file will be received only once from the client.
  - If we’re dealing with multiple files with the same schema (to start to ingest into our fixed layer), then it needs an “append” job running first to unify the data.
  
- Since the legacy data is coming from a single legacy system, I assumed that `facilityName` and `unitNumber` don't contain inconsistencies (e.g. other units in `rentRoll.csv` that are not in the `unit.csv`).
  - If they would present inconsistencies (most likely), some pre-processing would be required to unify the source-of-truth list. Business should be contacted for such occurrence, and another consequence is that we would need to enable `NULLS` for some columns, at least (and maybe other with further analysis):
    - `monument.rentalContract.unitId`
    - `monument.rentalContract.startDate`
    - `monument.unit.number`

- (Maybe not a good assumption), but I assumed you have a frequent data load into a `unit.csv` and a `rentRoll.csv` following the same csv table format over time, just appending or changing the data within, so I chose to load the data in a “bronze layer” for better data manipulation, debugging with SQL, and make it available for whatever other possible use downstream (usually an important step). But maybe we are not interested in storing client’s raw data in an RDBMS.

- I assumed `rentalInvoice.invoiceAmount` to be `monthlyRent`, due to lack of data, I thought it would make sense.

- `rentalInvoice.invoiceDueDate` = first date of the next month after `startDate`

**Helpers for the developemnt:**
- Official documentation mainly to check functions parameters because I already worked in very similar scripts with similar needs.
- PostgreSQL documentation and foruns for SQL syntax, and metadata references.

**Trade-off's with the chosen approach (read in github):**
| Python | SQL |
|---|---| 
| $${\color{red}harder}$$ to build dependency order in case of too many tables | $${\color{green}easier}$$ to build an automatic dependency graph  |
| $${\color{green}easier}$$ to do verifications before loading | $${\color{red}harder}$$ to import built-in validation steps in the process |
| $${\color{red}stores}$$ data in python runtime (may be expensive) | apply transformation rules $${\color{green}directly}$$ in the database (may be way more $${\color{green}performant}$$) |
| $${\color{red}costly}$$ to guarantee idempotency | $${\color{green}cheaper}$$ to guarantee idempotency (UPSERTs - MERGE Scripts) |
| $${\color{green}easier}$$ to run and integrate with programmatic workflows, environment and CI/CD | $${\color{red}harder}$$ to modularize and integrate with CI/CD, step-by-step automated process |
| $${\color{green}more}$$ observability throughout intermediate process | $${\color{red}less}$$ observability among intermediate steps

### Bonus section:

- Log any rejected/malformed rows for auditing.
  - A: The script is logging some rejected/malformed records for auditing purposes. E.g. names with more than 100 char are being printed. Proper logs are not setup in a separated file, although in many implementations this is a great approach.
- Make your script idempotent (re-runnable safely).
  - A: I couldn't make it happen due to only few knowledge about the data and I needed to understand more how an `UPSERT` could work in this case. I didn't want to implement a hard `TRUNCATE TABLE` logic since the "silver" tables are probably going to contain more data than whats being loaded by this script.




