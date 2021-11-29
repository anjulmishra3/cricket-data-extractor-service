from data_extractors.pulselive.table_schemas import connection, meta, engine, matches

import requests
import datetime


LIST_MATCHES_URL = "https://cricketapi.platform.iplt20.com//fixtures"

meta.create_all(engine)


def process_list_matches_response(response_dict, start_datetime):
    return_list = []

    for match in response_dict.get("content", []):
        # print(match.get("tournamentLabel", ""))
        if ("IPL " in match.get("tournamentLabel", "")) and ("Women" not in match.get("tournamentLabel", "")) and (datetime.datetime.strptime(match.get("timestamp").split("+")[0], "%Y-%m-%dT%H:%M:%S") < datetime.datetime.now()) and (datetime.datetime.strptime(match.get("timestamp").split("+")[0], "%Y-%m-%dT%H:%M:%S") > datetime.datetime.strptime(start_datetime, "%Y-%m-%dT%H:%M:%S")):
            details = match.get("scheduleEntry", {})
            match_dict = {
                "match_id": details.get("matchId", {}).get("id"),
                "match_name": details.get("matchId", {}).get("name"),
                "tournament_id": details.get("matchId", {}).get("tournamentId", {}).get("id"),
                "match_description": details.get("description"),
                "match_date": details.get("matchDate"),
                "match_type": details.get("matchType"),
                "match_state": details.get("matchState"),
                "match_highlights_link": details.get("highlightsLink"),
                "match_photos_link": details.get("photosLink"),
                "venue_id":  details.get("venue", {}).get("id"),
                "match_outcome": details.get("matchStatus", {}).get("outcome"),
                "match_outcome_text": details.get("matchStatus", {}).get("text"),
                "team_1_id": details.get("team1", {}).get("team", {}).get("id"),
                "team_1_innings_number": details.get("team1", {}).get("innings", [])[0].get("inningsNumber") if details.get("team1", {}).get("innings", []) else None,
                "team_1_innings_runs": details.get("team1", {}).get("innings", [])[0].get("runs") if details.get("team1", {}).get("innings", []) else None,
                "team_1_innings_wickets": details.get("team1", {}).get("innings", [])[0].get("wkts") if details.get("team1", {}).get("innings", []) else None,
                "team_1_innings_balls": details.get("team1", {}).get("innings", [])[0].get("ballsFaced") if details.get("team1", {}).get("innings", []) else None,
                "team_1_innings_maxballs": details.get("team1", {}).get("innings", [])[0].get("maxBalls") if details.get("team1", {}).get("innings", []) else None,
                "team_1_innings_allout": details.get("team1", {}).get("innings", [])[0].get("allOut") if details.get("team1", {}).get("innings", []) else None,
                "team_1_innings_declared": details.get("team1", {}).get("innings", [])[0].get("declared") if details.get("team1", {}).get("innings", []) else None,
                "team_1_innings_over_progress": details.get("team1", {}).get("innings", [])[0].get("overProgress") if details.get("team1", {}).get("innings", []) else None,
                "team_1_innings_run_rate": details.get("team1", {}).get("innings", [])[0].get("runRate") if details.get("team1", {}).get("innings", []) else None,
                "team_2_id": details.get("team2", {}).get("team", {}).get("id"),
                "team_2_innings_number": details.get("team2", {}).get("innings", [])[0].get("inningsNumber") if details.get("team2", {}).get("innings", []) else None,
                "team_2_innings_runs": details.get("team2", {}).get("innings", [])[0].get("runs") if details.get("team2", {}).get("innings", []) else None,
                "team_2_innings_wickets": details.get("team2", {}).get("innings", [])[0].get("wkts") if details.get("team2", {}).get("innings", []) else None,
                "team_2_innings_balls": details.get("team2", {}).get("innings", [])[0].get("ballsFaced") if details.get("team2", {}).get("innings", []) else None,
                "team_2_innings_maxballs": details.get("team2", {}).get("innings", [])[0].get("maxBalls") if details.get("team2", {}).get("innings", []) else None,
                "team_2_innings_allout": details.get("team2", {}).get("innings", [])[0].get("allOut") if details.get("team2", {}).get("innings", []) else None,
                "team_2_innings_declared": details.get("team2", {}).get("innings", [])[0].get("declared") if details.get("team2", {}).get("innings", []) else None,
                "team_2_innings_over_progress": details.get("team2", {}).get("innings", [])[0].get("overProgress") if details.get("team2", {}).get("innings", []) else None,
                "team_2_innings_run_rate": details.get("team2", {}).get("innings", [])[0].get("runRate") if details.get("team2", {}).get("innings", []) else None,
                "match_overs_limit": details.get("oversLimit"),
                "match_result_only": match.get("resultOnly"),
                "match_timestampMs": match.get("timestampMs"),
                "match_endTimestampMs": match.get("endTimestampMs"),
                "match_endTimestamp": match.get("endTimestamp"),
            }

            return_list.append(match_dict)

    return return_list


def list_matches(list_matches_url, start_datetime):

    params = {
        "page": 97,
        "pageSize": 300
    }
    final_matches_list = []

    response = requests.get(url=list_matches_url, params=params)
    if response.status_code == 200:
        response_dict = response.json()
    else:
        print(response.status_code, response.text)
        response_dict = {}

    pages = response_dict.get("pageInfo", {}).get("numPages") - 1
    # pages = 97

    final_matches_list.extend(process_list_matches_response(response_dict, start_datetime))
    # print(response_dict)

    while pages >= 1:
        params["page"] = pages
        # print(params)
        response = requests.get(url=list_matches_url, params=params)
        print("########################")
        print(response_dict.get("pageInfo", {}).get("page"))
        if response.status_code == 200:
            response_dict = response.json()
            if datetime.datetime.strptime(response_dict.get("content")[0].get("timestamp").split("+")[0],
                                          "%Y-%m-%dT%H:%M:%S") < datetime.datetime.strptime(start_datetime,
                                                                                            "%Y-%m-%dT%H:%M:%S"):
                print("Khatam")
                break
        else:
            response_dict = {}



        final_matches_list.extend(process_list_matches_response(response_dict, start_datetime))



        pages -= 1

        # print(len(final_matches_list))

    # print(set([match_id.get("team_1_id") for match_id in final_matches_list]))

    return final_matches_list


def insert_matches(matches_table, connection_object, start_datetime):

    matches_list = list_matches(LIST_MATCHES_URL, start_datetime)

    try:
        return_value = connection_object.execute(matches_table.insert(), matches_list)
    except Exception as e:
        print(e, matches_list[0], len(matches_list))
        return

    return return_value


insert_matches(matches, connection, "2021-09-01T00:00:00")
