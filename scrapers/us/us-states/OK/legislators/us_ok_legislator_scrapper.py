import us_ok_legislator_senate_scrapper as senate_scrapper
import us_ok_legislator_house_scrapper as house_scrapper

def main():
    print('OKLAHOMA!')
    print('We know we belong to the land ♫ ♫ ♫')
    print('And the land we belong to is grand! ♫ ♫ ♫\n')

    senate_scrapper.scrape_senate_legislators()
    house_scrapper.scrape_house_legislators()

    print('\nCOMPLETE!')


if __name__ == '__main__':
    main()