from data_extractors.pulselive.table_schemas import engine, meta, players, connection
from sqlalchemy import text

import requests


LIST_PLAYERS_URL = "https://cricketapi-icc.pulselive.com/stats/players"

meta.create_all(engine)


def list_team_ids():
    team_id_query = text("select team_id from teams")
    team_ids = connection.execute(team_id_query).fetchall()

    return [str(team[0]) for team in team_ids]


def list_tournament_ids():
    tournament_id_query = text("select tournament_id from tournaments")
    tournament_ids = connection.execute(tournament_id_query).fetchall()

    return [str(tournament[0]) for tournament in tournament_ids]


def process_list_players(response_dict, tournament_id):
    player_dicts = response_dict["stats"]["content"]
    response_list = []

    for player in player_dicts:
        stats = player["stats"][-1] if player["stats"] else {}
        try:
            player_dict = {
                "player_id": player["player"]["id"],
                "tournament_id": int(tournament_id),
                "full_name": player.get("player", {}).get("fullName"),
                "short_name": player.get("player", {}).get("shortName"),
                "nationality": player.get("player", {}).get("nationality"),
                "date_of_birth": player.get("player", {}).get("dateOfBirth"),
                "right_armed_bowl": player.get("player", {}).get("rightArmedBowl"),
                "right_handed_bat": player.get("player", {}).get("rightHandedBat"),
                "bowling_style": player.get("player", {}).get("bowlingStyle"),
                "batting_50s": stats.get("battingStats", {}).get("50s"),
                "batting_100s": stats.get("battingStats", {}).get("100s"),
                "batting_innings": stats.get("battingStats", {}).get("inns"),
                "batting_matches": stats.get("battingStats", {}).get("m"),
                "batting_runs": int(stats.get("battingStats", {}).get("r")) if stats.get("battingStats", {}).get("r") and (stats.get("battingStats", {}).get("r") != "-") else None,
                "batting_balls": stats.get("battingStats", {}).get("b"),
                "batting_4s": stats.get("battingStats", {}).get("4s"),
                "batting_6s": stats.get("battingStats", {}).get("6s"),
                "batting_notouts": stats.get("battingStats", {}).get("no"),
                "batting_high_score": stats.get("battingStats", {}).get("hs"),
                "batting_strike_rate": float(stats.get("battingStats", {}).get("sr")) if stats.get("battingStats", {}).get("sr") and (stats.get("battingStats", {}).get("sr") != "-") else None,
                "batting_average": float(stats.get("battingStats", {}).get("a")) if stats.get("battingStats", {}).get("a") and (stats.get("battingStats", {}).get("a") != "-") else None,
                "bowling_bbiw": stats.get("bowlingStats", {}).get("bbiw"),
                "bowling_bbir": stats.get("bowlingStats", {}).get("bbir"),
                "bowling_bbmw": stats.get("bowlingStats", {}).get("bbmw"),
                "bowling_bbmr": stats.get("bowlingStats", {}).get("bbmr"),
                "bowling_4w": stats.get("bowlingStats", {}).get("4w"),
                "bowling_5w": stats.get("bowlingStats", {}).get("5w"),
                "bowling_10w": stats.get("bowlingStats", {}).get("10w"),
                "bowling_innings": stats.get("bowlingStats", {}).get("inns"),
                "bowling_matches": stats.get("bowlingStats", {}).get("m"),
                "bowling_balls": stats.get("bowlingStats", {}).get("b"),
                "bowling_runs": stats.get("bowlingStats", {}).get("r"),
                "bowling_wide_balls": stats.get("bowlingStats", {}).get("wb"),
                "bowling_no_balls": stats.get("bowlingStats", {}).get("nb"),
                "bowling_dots": stats.get("bowlingStats", {}).get("d"),
                "bowling_wickets": stats.get("bowlingStats", {}).get("w"),
                "bowling_4s": stats.get("bowlingStats", {}).get("4s"),
                "bowling_6s": stats.get("bowlingStats", {}).get("6s"),
                "bowling_maidens": stats.get("bowlingStats", {}).get("maid"),
                "bowling_wmaidens": stats.get("bowlingStats", {}).get("wmaid"),
                "bowling_hat_tricks": stats.get("bowlingStats", {}).get("ht"),
                "bowling_average": float(stats.get("bowlingStats", {}).get("a")) if stats.get("bowlingStats", {}).get("a") and (stats.get("bowlingStats", {}).get("a") != "-") else None,
                "bowling_economy": float(stats.get("bowlingStats", {}).get("e")) if stats.get("bowlingStats", {}).get("e") and (stats.get("bowlingStats", {}).get("e") != "-") else None,
                "bowling_sr": float(stats.get("bowlingStats", {}).get("sr")) if stats.get("bowlingStats", {}).get("sr") and (stats.get("bowlingStats", {}).get("sr") != "-") else None,
                "bowling_overs": float(stats.get("bowlingStats", {}).get("o")) if stats.get("bowlingStats", {}).get("o") and (stats.get("bowlingStats", {}).get("o") != "-") else None,
                "fielding_catches": stats.get("fieldingStats", {}).get("c"),
                "fielding_runouts": stats.get("fieldingStats", {}).get("ro"),
                "fielding_stumpings": stats.get("fieldingStats", {}).get("s"),
                "fielding_innings": stats.get("fieldingStats", {}).get("inns"),
                "fielding_matches": stats.get("fieldingStats", {}).get("m")
            }
        except Exception as e:
            print(e, player)
            continue
        response_list.append(player_dict)

    return response_list


def list_players(list_players_url):

    team_ids = list_team_ids()
    tournament_ids = list_tournament_ids()
    players_list = []

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

        players_list.extend(process_list_players(response_dict, tournament_id))

        pages = response_dict.get("stats", {}).get("pageInfo", {}).get("numPages", 0)
        # print(players_list)
        # break
        while page + 1 < pages:
            page += 1
            params["page"] = page
            response = requests.get(url=list_players_url, params=params)

            response_dict = response.json() if response.status_code == 200 else None

            players_list.append(process_list_players(response_dict, tournament_id))

    return players_list


def insert_players(players_table, connection_object):

    players_list = list_players(LIST_PLAYERS_URL)

    try:
        return_value = connection_object.execute(players_table.insert(), players_list)
    except Exception as e:
        print(e, players_list[0], len(players_list))
        return

    return return_value


insert_players(players, connection)
