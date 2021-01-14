## Goverlytics Scrapers

### Templates
#### Important
If using the template, be sure to copy the template file. Do not use or modify the original template files. Also, at the project root, you will find 4 modules necessary for the templates to work: `database_tables`, `database`, `legislation_scraper_utils`, and `legislator_scraper_utils`. Please do not modify these files, as there should be no need to. If you need to modify them, you can extend them in your scraper and modify as necessary.

You must scrape the `legislator` data first. The reason for this is the `legislation` table depends on data from the `legislator` table, specifically the `goverlytics_id`.

Be sure to check out the `template_demo` folder as well. This showcases the template in action. You can run it as well. If you'd prefer, you can copy this and use it in your project rather than the vanilla `template` folder.

The template may not be suitable for every website, and is more of a guideline that should work for a sizable portion of the sites we scrape. The `scraper_utils` methods, such as `initialize_row()`, `get_party_id()`, and `insert_legislator_data_into_db()`, may still be useful if you need to make heavy modifications. Once you copy the template, you are free to modify it however you see fit.

#### Usage
- Copy the `template` folder. Be sure it is in the `scrapers/us-states` directory. Rename the copied folder to the state abbreviation of the state you are building a scraper for.
- Go to the copied folder, then open the `config.cfg`. Change the `state_abbreviation`, `database_table_name`, and `country` field to the appropriate value. Database table name should be in the following format: `us_<state-abbreviation>_<dataset>`. Eg: `us_ca_legislators`
- Go to the project root (the one containing the `requirements.txt` file) and create a virtual environment:
```bash
python3 -m venv venv
```
- Activate virtual environment:
```bash
# WINDOWS:
venv\Scripts\activate
# MAC/LINUX:
. venv/bin/activate
```
- Then install dependencies using:
```bash
pip install -r requirements.txt
```
You're now able to begin working on the scraper! Hopefully the comments in the template are helpful but feel free to delete them if they get in the way... and as always, please let me know if you have any questions!

#### Adding libraries
If you require a certain library in your scraper not included already, add it to the `requirements.txt` at the project root, then run `pip install -r requirements.txt` again after activating the virtual environment. You should now be able to import the library into your scraper.

#### Stuff to do:
- Add legislation template and template demo
- Add Selenium support