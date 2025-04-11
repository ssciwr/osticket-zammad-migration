from .osticket import get_osticket_data
from .zammad import upload_zammad_data


if __name__ == '__main__':
    osticket_df = get_osticket_data()
    print(osticket_df)
