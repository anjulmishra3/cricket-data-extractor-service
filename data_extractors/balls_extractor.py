from data_extractors.table_schemas import connection, meta, engine, balls
from pymysql.err import IntegrityError
from sqlalchemy import text

import requests
import datetime



LIST_MATCHES_URL = "https://cricketapi.platform.iplt20.com//fixtures/"


meta.create_all(engine)



def process_list_balls_response(response_dict, match_id, tournament_id, venue_id, team_1_id, team_2_id):
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

                    "team_id": team_1_id if int(key.split(".")[0]) == 1 else team_2_id,
                    "over_number": int(key.split(".")[1]) - 1,
                    "ball_number": int(key.split(".")[2]),
                    "innings_number": int(key.split(".")[0]),
                    "over_ball": int(values[0]),
                    "batsman": int(values[1]),
                    "non_striker": int(values[2]),
                    "bowler": int(values[3]),
                    "speed_kmph": 3.6 * float(values[4]),
                    "catcher": int(values[5]),
                    "dismissal_description": values[6],
                    "total_extras": int(values[7]),
                    "runs": int(values[8]),
                    "bowler_extras": int(values[9]),
                    "extra_type": values[10],
                    "otw": True if values[11] == "y" else False,
                    "length_on_pitching": float(values[12]) if values[12] else None,
                    "line_on_pitching": float(values[13]) if values[13] else None,
                    "line_at_stumps": float(values[14]) if values[14] else None,
                    "height_at_stumps": float(values[15]) if values[15] else None,
                    "shot_x": float(values[16]) if values[16] else None,
                    "shot_y": float(values[17]) if values[17] else None
                }
            else:
                ball_dict = {}

            balls_list.append(ball_dict)

    return balls_list


def list_balls(match_details, list_balls_baseurl):
    final_balls_list = []

    for match in match_details:
        balls_url = list_balls_baseurl + f"{match[0]}/uds/stats"

        response = requests.get(balls_url)

        if response.status_code == 200:
            response_dict = response.json()
        else:
            print(response.status_code, response.text)
            response_dict = {}

        final_balls_list.extend(process_list_balls_response(response_dict, match[0], match[1], match[2], match[3], match[4]))

    return final_balls_list


def list_match_details():
    matches_query = text("select match_id, tournament_id, venue_id, team_1_id, team_2_id from matches")
    match_details = connection.execute(matches_query).fetchall()

    return [list(match_detail) for match_detail in match_details]


def insert_balls(balls_table, connection_object):

    failed_rows = []

    matches = list_match_details()

    balls_list = list_balls(matches, LIST_MATCHES_URL)

    # print(set([ball["non_striker"] for ball in balls_list]))

    try:
        return_value = connection_object.execute(balls_table.insert(), balls_list)
    except Exception as e:
        print(e, balls_list[0], len(balls_list))
        return
    except IntegrityError as e:
        print(e)
        return
        # failed_rows.append()

    return return_value


insert_balls(balls, connection)
