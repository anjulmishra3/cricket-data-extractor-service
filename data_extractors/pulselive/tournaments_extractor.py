from data_extractors.pulselive.table_schemas import connection, tournaments, meta, engine

import datetime
import requests


LIST_TOURNAMENTS_URL = "https://cricketapi.platform.iplt20.com/tournaments"

START_YEAR = 2020

meta.create_all(engine)


def list_tournaments(list_tournaments_url, start_year):
    current_year = datetime.datetime.now().year
    start_dates = [str(f"{year}-06-14") for year in range(start_year, current_year)]
    tournaments_list = []

    for start_date in start_dates:
        params = {
            "page": 0,
            "pageSize": 50,
            "startDate": start_date
        }
        response = requests.get(url=list_tournaments_url, params=params)
        if response.status_code != 200:
            print(response.status_code)
        response = response.json() if response.status_code == 200 else None

        if response:
            for tournament in response.get("content", []):
                # print(tournament.get("description", ""), f"IPL {start_date.split('-')[0]}")
                if tournament.get("description", "") == f"IPL {start_date.split('-')[0]}":
                    tournament_dict = {
                        "tournament_id": tournament["id"],
                        "name": tournament["name"],
                        "description": tournament["description"],
                        "type": tournament["tournamentType"],
                        "provisional": tournament["provisional"],
                        "start_date": tournament["startDate"],
                        "end_date": tournament["endDate"],
                        "number_of_matches": tournament["matchesByType"]["IPLT20"],
                    }

                    tournaments_list.append(tournament_dict)
                    break

    return tournaments_list


def insert_tournaments(tournaments_table, connection_object):

    tournaments_list = list_tournaments(LIST_TOURNAMENTS_URL, START_YEAR)

    return_value = connection_object.execute(tournaments_table.insert(), tournaments_list)

    return return_value


insert_tournaments(tournaments, connection)
