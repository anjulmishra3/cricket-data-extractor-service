from data_extractors.table_schemas import connection, meta, engine, matches

import requests
import datetime


LIST_MATCHES_URL = "https://cricketapi.platform.iplt20.com//fixtures/689/uds/stats"

meta.create_all(engine)


def process_list_balls_response(response_dict, match_id, tournament_id, venue_id, team_id):
    balls_list = []
    details = response_dict.get("data", [])
    for detail in details:
        for key, value in detail.items():
            if key and value:
                values = value.split(",")
                ball_dict = {
                    "match_id": match_id,
                    "tournament_id": tournament_id,
                    "venue_id": venue_id,
                    "team_id": team_id,
                    "over": int(key.split(".")[1]),
                    "ball_num": int(key.split(".")[2]),
                    "innings": int(key.split(".")[0]),
                    "batter": int(values[1]),
                    "non_striker": int(values[2]),
                    "bowler": int(values[3]),
                    "speed":3.6 * float(values[4]),
                    "catcher": int(values[5]),
                    "dismissal_desc": values[6],
                    "total_extras": int(values[7]),
                    "runs": int(values[8]),
                    "bowler_extras": int(values[9]),
                    "extra_type": values[10],
                    "otw": True if  values[11] == "y" else False,
                    "length": float(values[12]),
                    "line": float(values[13]),
                    "line_at_stumps": float(values[14]),
                    "height_at_stumps": float(values[15]),
                    "shot_dist0": float(values[16]),
                    "shot_dist1": float(values[17]),
                    "blank2": float(values[18]),
                    "blank3": float(values[19]),
                    "blank4": float(values[20]),
                }
            else:
                ball_dict = {}

            balls_list.append(ball_dict)

    return balls_list


def list_balls(match_id, tournament_id, venue_id, list_balls_baseurl):
    final_balls_list = []

    balls_url = list_balls_baseurl + f"{match_id}/uds/stats"

    response = requests.get(balls_url)

    if response.status_code == 200:
        response_dict = response.json()
    else:
        print(response.status_code, response.text)
        response_dict = {}

    final_balls_list.extend(process_list_balls_response(response_dict, match_id, tournament_id, venue_id, team_id))