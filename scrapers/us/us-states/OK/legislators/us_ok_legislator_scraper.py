import us_ok_legislator_senate_scraper as senate_scraper
import us_ok_legislator_house_scraper as house_scraper

def main():
    print('OKLAHOMA!')
    print('We know we belong to the land ♫ ♫ ♫')
    print('And the land we belong to is grand! ♫ ♫ ♫\n')

    print('\nSCRAPING OKLAHOMA LEGISLATORS\n')

    senate_scraper.scrape_senate_legislators()
    house_scraper.scrape_house_legislators()

    print('\nCOMPLETE!\n')

if __name__ == '__main__':
    main()