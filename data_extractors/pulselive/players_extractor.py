from data_extractors.pulselive.table_schemas import engine, meta, players, connection
from sqlalchemy import text

import requests


LIST_PLAYERS_URL = "https://cricketapi-icc.pulselive.com/stats/players"

meta.create_all(engine)

players_dict = {}


def list_team_ids():
    team_id_query = text("select team_id from teams")
    team_ids = connection.execute(team_id_query).fetchall()

    return [str(team[0]) for team in team_ids]


def list_tournament_ids():
    tournament_id_query = text("select tournament_id from tournaments")
    tournament_ids = connection.execute(tournament_id_query).fetchall()

    return [str(tournament[0]) for tournament in tournament_ids]


def process_list_players(response_dict):
    player_dicts = response_dict["stats"]["content"]

    for player in player_dicts:
        try:
            players_dict[player["player"]["id"]] = {
                "player_id": player["player"]["id"],
                "full_name": player.get("player", {}).get("fullName"),
                "short_name": player.get("player", {}).get("shortName"),
                "nationality": player.get("player", {}).get("nationality"),
                "date_of_birth": player.get("player", {}).get("dateOfBirth"),
                "right_armed_bowl": player.get("player", {}).get("rightArmedBowl"),
                "right_handed_bat": player.get("player", {}).get("rightHandedBat"),
                "bowling_style": player.get("player", {}).get("bowlingStyle"),
            }
        except Exception as e:
            print(e, player)
            continue

    # return players_dict


def list_players(list_players_url, tournament_ids):

    team_ids = list_team_ids()
    tournament_ids = list_tournament_ids() if not tournament_ids else tournament_ids


    for tournament_id in tournament_ids:
        page = 0
        params = {
            "teamIds": ", ".join(team_ids),
            "tournamentIds": tournament_id,
            "pageSize": 100
        }
        response = requests.get(url=list_players_url, params=params)

        response_dict = response.json() if response.status_code == 200 else None

        # if response.status_code != 200:
        #     print(response.status_code, response.text)

        # players_dict = process_list_players(response_dict)
        process_list_players(response_dict)

        pages = response_dict.get("stats", {}).get("pageInfo", {}).get("numPages", 0)
        # print(players_list)
        # break
        while page + 1 < pages:
            page += 1
            params["page"] = page
            response = requests.get(url=list_players_url, params=params)

            response_dict = response.json() if response.status_code == 200 else None

            # players_dict = process_list_players(response_dict, players_dict)
            process_list_players(response_dict)

    # return players_dict


def insert_players(players_table, connection_object, bulk_ingest):

    # players_dict = list_players(LIST_PLAYERS_URL)
    list_players(LIST_PLAYERS_URL, tournament_ids=[22399])

    if not bulk_ingest:
        for player in players_dict.values():
            try:
                return_value = players_table.insert().values(player)
                return_value = connection_object.execute(return_value)
                print(player["full_name"], return_value)
            except Exception as e:
                # print(player["full_name"])
                pass
    else:
        try:
            return_value = connection_object.execute(players_table.insert(), list(players_dict.values()))
        except Exception as e:
            print(e, list(players_dict.values())[0], len(list(players_dict.values())))
            return

    return return_value


insert_players(players, connection, bulk_ingest=False)
