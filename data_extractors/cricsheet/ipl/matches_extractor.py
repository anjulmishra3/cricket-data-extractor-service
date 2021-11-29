import json
import os
from pymysql.err import IntegrityError
from sqlalchemy import Table, Column, Integer, String, MetaData, Date, create_engine

key = ""

meta = MetaData()
req_directory = os.environ.get("CRICSHEET_IPL_FILES_DIRECTORY")


class CricsheetMatch:
    def __init__(self):
        self.engine = create_engine(
            "mysql+pymysql://root:password@localhost:3306/cricsheet_ipl"
        )
        self.connection = self.engine.connect()
        self.table = Table(
            "matches",
            meta,
            Column("match_id", Integer, default=0, primary_key=True),
            Column("venue", String(40), default=""),
            Column("city", String(40), default=""),
            Column("dates", Date),
            Column("event_name", String(40), default=""),
            Column("event_match_number", String(40), default=""),
            Column("gender", String(40), default=""),
            Column("match_type", String(40), default=""),
            Column("umpire_1", String(40), default=""),
            Column("umpire_2", String(40), default=""),
            Column("winner", String(40), default=""),
            Column("outcome_type", String(40), default=""),
            Column("outcome_margin", String(40), default=""),
            Column("player_of_match", String(40), default=""),
            Column("season", String(40), default=""),
            Column("team_type", String(40), default=""),
            Column("team_1", String(40), default=""),
            Column("team_2", String(40), default=""),
            Column("toss_winner", String(40), default=""),
            Column("toss_decision", String(40), default=""),
        )

    def bulk_insert_matches(self, data):
        try:
            response = self.connection.execute(self.table.insert(), data)
        except IntegrityError as e:
            print(e)
            return

        return response

    def process_json_to_data(self, data, filename):
        match_info = data.get("info", {})
        processed_data = {
            "match_id": filename.split(".")[0],
            "venue": match_info.get("venue"),
            "city": match_info.get("city"),
            "dates": match_info.get("dates")[0],
            "event_name": match_info.get("event", {}).get("name"),
            "event_match_number": match_info.get("event", {}).get("match_number"),
            "gender": match_info.get("gender"),
            "match_type": match_info.get("match_type"),
            "umpire_1": match_info.get("officials", {}).get("umpires")[0] if match_info.get("officials", {}) else None,
            "umpire_2": match_info.get("officials", {}).get("umpires")[1] if match_info.get("officials", {}) else None,
            "winner": match_info.get("outcome", {}).get("winner"),
            "outcome_type": [key for key in match_info.get("outcome", {}).get("by", {}).keys()][0] if match_info.get("outcome", {}).get("by", {}) else "tie",
            "outcome_margin": [value for value in match_info.get("outcome", {}).get("by", {}).values()][0] if match_info.get("outcome", {}).get("by", {}) else None,
            "player_of_match": match_info.get("player_of_match")[0] if match_info.get("player_of_match") else None,
            "season": match_info.get("season"),
            "team_type": match_info.get("team_type"),
            "team_1": match_info.get("teams")[0],
            "team_2": match_info.get("teams")[1],
            "toss_winner": match_info.get("toss", {}).get("winner"),
            "toss_decision": match_info.get("toss", {}).get("decision"),
        }

        return processed_data


def open_json_file(filepath):
    try:
        with open(filepath) as f:
            data = json.loads(f.read())
        return data
    except Exception as e:
        print(e, filepath)
        return None


def main(directory):
    cricsheet_match = CricsheetMatch()

    for filename in os.listdir(directory):
        file_data = open_json_file(directory + filename)
        if not file_data:
            continue

        try:
            data = cricsheet_match.process_json_to_data(file_data, filename)
        except Exception as e:
            print(filename, e)
            continue
        insertion = cricsheet_match.bulk_insert_matches(data)


main(req_directory)
